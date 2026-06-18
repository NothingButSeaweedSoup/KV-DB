package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ByteUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WALManager {

    private static final Logger log = LoggerFactory.getLogger(WALManager.class);

    /** BATCH模式默认每多少条记录触发一次fsync */
    private static final int DEFAULT_BATCH_INTERVAL = 100;
    /** BATCH模式默认定时fsync间隔（毫秒） */
    private static final long DEFAULT_BATCH_INTERVAL_MS = 1000;
    /** ASYNC模式默认定时fsync间隔（毫秒） */
    private static final long DEFAULT_ASYNC_INTERVAL_MS = 1000;

    private final String walPath;
    private final long segmentSize;
    private final FsyncStrategy fsyncStrategy;
    private final int batchInterval;
    private final long batchIntervalMs;
    private final Object channelLock = new Object();
    private volatile FileChannel channel;
    private long currentSize;
    private final ExecutorService compressor;
    private final AtomicInteger writeCount = new AtomicInteger(0);
    private ScheduledExecutorService fsyncScheduler;

    /**
     * 使用默认BATCH策略创建WALManager
     */
    public WALManager(String walPath, long segmentSize) throws IOException {
        this(walPath, segmentSize, FsyncStrategy.BATCH);
    }

    /**
     * 使用指定fsync策略创建WALManager
     */
    public WALManager(String walPath, long segmentSize, FsyncStrategy fsyncStrategy) throws IOException {
        this(walPath, segmentSize, fsyncStrategy, DEFAULT_BATCH_INTERVAL, DEFAULT_BATCH_INTERVAL_MS);
    }

    /**
     * 使用完全自定义参数创建WALManager
     *
     * @param walPath         WAL目录路径
     * @param segmentSize     段大小（字节）
     * @param fsyncStrategy   fsync策略
     * @param batchInterval   BATCH模式下每隔多少条记录触发fsync
     * @param batchIntervalMs BATCH/ASYNC模式下定时fsync间隔（毫秒）
     */
    public WALManager(String walPath, long segmentSize, FsyncStrategy fsyncStrategy,
                      int batchInterval, long batchIntervalMs) throws IOException {
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("WAL段大小必须大于0");
        }
        if (fsyncStrategy == null) {
            throw new IllegalArgumentException("fsyncStrategy不能为null");
        }

        this.segmentSize = segmentSize;
        this.fsyncStrategy = fsyncStrategy;
        this.batchInterval = batchInterval;
        this.batchIntervalMs = batchIntervalMs;
        this.walPath = walPath + Constants.System.FILE_SEPARATOR + "WAL" + Constants.File.WAL_EXTENSION;

        File walDir = new File(walPath);
        if (!walDir.exists() && !walDir.mkdirs()) {
            throw new IOException("创建WAL目录失败");
        }
        this.compressor = Executors.newFixedThreadPool(2);

        initFsyncScheduler();
    }

    public void init(RecoveryCallback callback) throws IOException {
        File walFile = new File(walPath);
        if (walFile.exists()) {
            try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.READ)) {
                recover(channel, callback);
            }
        }
        openWAL();
    }

    private void initFsyncScheduler() {
        if (fsyncStrategy == FsyncStrategy.SYNC) {
            return;
        }

        long intervalMs = fsyncStrategy == FsyncStrategy.ASYNC ? DEFAULT_ASYNC_INTERVAL_MS : batchIntervalMs;
        fsyncScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wal-fsync-" + fsyncStrategy.name().toLowerCase());
            t.setDaemon(true);
            return t;
        });
        fsyncScheduler.scheduleAtFixedRate(() -> {
            try {
                synchronized (channelLock) {
                    if (channel != null && channel.isOpen()) {
                        channel.force(true);
                    }
                }
            } catch (IOException e) {
                log.warn("定时fsync失败", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("WAL fsync策略: {}, 定时间隔: {}ms", fsyncStrategy, intervalMs);
    }

    public void log(byte operation, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        WALEntry entry = new WALEntry(operation, key, value);
        ByteBuffer buffer = entry.serialize();
        int entrySize = buffer.remaining();

        synchronized (channelLock) {
            if (currentSize + entrySize > segmentSize) {
                rotateWAL();
            }

            int bytesWritten = channel.write(buffer);
            currentSize += bytesWritten;

            switch (fsyncStrategy) {
                case SYNC -> channel.force(true);
                case BATCH -> {
                    int count = writeCount.incrementAndGet();
                    if (count % batchInterval == 0) {
                        channel.force(true);
                    }
                }
                case ASYNC -> {
                    // 由后台定时线程负责fsync
                }
            }
        }
    }

    private void openWAL() throws IOException {
        File walFile = new File(walPath);

        channel = FileChannel.open(
                walFile.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );

        if (walFile.exists()) {
            currentSize = channel.size();
        } else {
            currentSize = 0;
        }
    }

    /**
     * @param channel
     * @param callback
     * @throws IOException 按照格式，从WAL日志文件中读取操作类型、key长度、key值、value长度和value值，并调用回调函数处理每个日志条目
     */
    private void recover(FileChannel channel, RecoveryCallback callback) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long position = 0;
        long fileSize = channel.size();

        while (position < fileSize) {
            channel.position(position);

            buffer.clear().limit(1);
            int bytesRead = channel.read(buffer);
            if (bytesRead != 1) {
                break;
            }
            buffer.flip();
            byte operation = buffer.get();

            buffer.clear().limit(4);
            bytesRead = channel.read(buffer);
            if (bytesRead != 4) {
                break;
            }
            buffer.flip();
            int keyLength = buffer.getInt();

            buffer = ensureCapacity(buffer, keyLength);
            buffer.clear().limit(keyLength);
            bytesRead = channel.read(buffer);
            if (bytesRead != keyLength) {
                break;
            }
            buffer.flip();
            byte[] key = new byte[keyLength];
            buffer.get(key);

            buffer.clear().limit(4);
            bytesRead = channel.read(buffer);
            if (bytesRead != 4) {
                break;
            }
            buffer.flip();
            int valueLength = buffer.getInt();

            byte[] value = null;
            if (valueLength > 0) {
                buffer = ensureCapacity(buffer, valueLength);
                buffer.clear().limit(valueLength);
                bytesRead = channel.read(buffer);
                if (bytesRead != valueLength) {
                    break;
                }
                buffer.flip();
                value = new byte[valueLength];
                buffer.get(value);
            }
            position += 1 + 4 + keyLength + 4 + valueLength;

            callback.recoveryEntry(operation, key, value);
        }
    }

    private ByteBuffer ensureCapacity(ByteBuffer buffer, int required) {
        if (buffer.capacity() >= required) {
            return buffer;
        }
        return ByteBuffer.allocate(required);
    }

    private void rotateWAL() throws IOException {
        channel.close();

        File currentWAL = new File(walPath);
        String newName = walPath + "_" + System.currentTimeMillis();
        File targetFile = new File(newName);

        // Windows 上文件重命名可能因句柄未释放而失败，短暂重试
        boolean moved = false;
        IOException lastException = null;
        for (int i = 0; i < 10; i++) {
            try {
                Files.move(currentWAL.toPath(), targetFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                moved = true;
                break;
            } catch (IOException e) {
                lastException = e;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (!moved) {
            openWAL();
            throw new IOException("无法重命名WAL文件", lastException);
        }

        compressor.submit(() -> {
            try {
                compactWAL(targetFile);
            } catch (IOException e) {
                log.error("WAL压缩失败", e);
            }
        });
        openWAL();
    }

    private void compactWAL(File walFile) throws IOException {
        Map<byte[], WALEntry> operations = new HashMap<>();
        try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            long position = 0;
            long fileSize = channel.size();
            while (position < fileSize) {
                buffer.clear().limit(1);
                int bytesRead = channel.read(buffer);
                if (bytesRead != 1) {
                    break;
                }
                buffer.flip();
                byte operation = buffer.get();

                buffer.clear().limit(4);
                bytesRead = channel.read(buffer);
                if (bytesRead != 4) {
                    break;
                }
                buffer.flip();
                int keyLength = buffer.getInt();

                buffer = ensureCapacity(buffer, keyLength);
                buffer.clear().limit(keyLength);
                bytesRead = channel.read(buffer);
                if (bytesRead != keyLength) {
                    break;
                }
                buffer.flip();
                byte[] key = new byte[keyLength];
                buffer.get(key);

                position += 1 + 4 + keyLength;

                buffer.clear().limit(4);
                bytesRead = channel.read(buffer);
                if (bytesRead != 4) {
                    break;
                }
                buffer.flip();
                int valueLength = buffer.getInt();

                byte[] value = null;
                if (valueLength > 0) {
                    buffer = ensureCapacity(buffer, valueLength);
                    buffer.clear().limit(valueLength);
                    bytesRead = channel.read(buffer);
                    if (bytesRead != valueLength) {
                        break;
                    }
                    buffer.flip();
                    value = new byte[valueLength];
                    buffer.get(value);
                }
                operations.put(key, new WALEntry(operation, key, value));
                position += 4 + valueLength;
            }
        } catch (IOException e) {
            log.error("读取WAL文件失败: {}", walFile.getPath(), e);
            throw e;
        }

        try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (WALEntry entry : operations.values()) {
                channel.write(entry.serialize());
            }
        } catch (IOException e) {
            log.error("写入压缩后的WAL文件失败: {}", walFile.getPath(), e);
            throw e;
        }
    }

    public void clear() throws IOException {
        synchronized (channelLock) {
            if (channel != null) {
                channel.close();
            }
            // 删除旧的WAL文件
            for (File file : Objects.requireNonNull(new File(walPath).getParentFile().listFiles())) {
                if (file.getName().matches(".*\\.wal(_\\d+)?")) {
                    file.delete();
                }
            }

            // 重新打开WAL文件，截断为0
            channel = FileChannel.open(
                    new File(walPath).toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

            currentSize = 0;
        }
    }

    public void close() throws IOException {
        // 先关闭调度器，停止后台fsync
        if (fsyncScheduler != null) {
            fsyncScheduler.shutdown();
        }
        synchronized (channelLock) {
            if (channel != null) {
                // 关闭前确保所有数据已刷盘
                channel.force(true);
                channel.close();
                channel = null;
            }
        }
        compressor.shutdown();
    }

    public String getWalPath() {
        return walPath;
    }

    public interface RecoveryCallback {
        void recoveryEntry(byte operation, byte[] key, byte[] value) throws IOException;
    }

    public class WALEntry {
        private static final int LOG_ENTRY_SIZE = 1 + 4 + 4;

        private final byte operation;
        private final byte[] key;
        private final byte[] value;

        public WALEntry(byte operation, byte[] key, byte[] value) {
            this.operation = operation;
            this.key = key;
            this.value = value;
        }

        public ByteBuffer serialize() {
            int valueLength = value == null ? 0 : value.length;
            ByteBuffer buffer = ByteBuffer.allocate(LOG_ENTRY_SIZE + key.length + valueLength);

            buffer.put(operation);
            buffer.putInt(key.length);
            buffer.put(key);
            buffer.putInt(valueLength);
            if (value != null) {
                buffer.put(value);
            }
            buffer.flip();
            return buffer;
        }

        public String toBytes() {
            return "WALEntry{"
                    + "operation=" + operation + ", "
                    + "keyLength=" + key.length + ", "
                    + "valueLength=" + (value != null ? value.length : 0) + ", "
                    + "}";
        }

        public byte getOperation() {
            return operation;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }
}
