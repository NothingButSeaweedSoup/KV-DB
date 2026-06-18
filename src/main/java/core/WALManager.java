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
import java.util.zip.CRC32;

public class WALManager {

    private static final Logger log = LoggerFactory.getLogger(WALManager.class);

    /** WAL 文件版本魔数: 0x4B564442 ("KVDB") */
    private static final int WAL_MAGIC = 0x4B564442;
    /** WAL 文件头大小（魔数 4 字节） */
    private static final int WAL_HEADER_SIZE = 4;

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
        if (walFile.exists() && walFile.length() > 0) {
            try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.READ)) {
                // 检测 WAL 格式版本
                ByteBuffer magicBuf = ByteBuffer.allocate(WAL_HEADER_SIZE);
                int bytesRead = channel.read(magicBuf);
                if (bytesRead == WAL_HEADER_SIZE) {
                    magicBuf.flip();
                    int magic = magicBuf.getInt();
                    if (magic == WAL_MAGIC) {
                        // 新格式：带 CRC32
                        recover(channel, callback, WAL_HEADER_SIZE);
                    } else {
                        // 旧格式：无 CRC32，回退到文件开头
                        log.info("检测到旧格式WAL文件，使用兼容模式恢复");
                        recoverLegacy(channel, callback);
                    }
                }
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
        boolean isNew = !walFile.exists() || walFile.length() == 0;

        channel = FileChannel.open(
                walFile.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );

        if (isNew) {
            // 新文件写入版本魔数头
            ByteBuffer header = ByteBuffer.allocate(WAL_HEADER_SIZE);
            header.putInt(WAL_MAGIC);
            header.flip();
            channel.write(header);
            currentSize = WAL_HEADER_SIZE;
        } else {
            currentSize = channel.size();
        }
    }

    /**
     * @param channel
     * @param callback
     * @throws IOException 按照格式，从WAL日志文件中读取操作类型、key长度、key值、value长度和value值，并调用回调函数处理每个日志条目
     */
    /**
     * 恢复新格式 WAL（带 CRC32 校验）。
     */
    private void recover(FileChannel channel, RecoveryCallback callback, long startPosition) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long position = startPosition;
        long fileSize = channel.size();

        while (position < fileSize) {
            channel.position(position);

            // 读取 CRC32 (4 bytes)
            buffer.clear().limit(4);
            int bytesRead = channel.read(buffer);
            if (bytesRead != 4) {
                break;
            }
            buffer.flip();
            int storedCrc = buffer.getInt();

            // 读取 operation (1 byte)
            buffer.clear().limit(1);
            bytesRead = channel.read(buffer);
            if (bytesRead != 1) {
                break;
            }
            buffer.flip();
            byte operation = buffer.get();

            // 读取 keyLength (4 bytes)
            buffer.clear().limit(4);
            bytesRead = channel.read(buffer);
            if (bytesRead != 4) {
                break;
            }
            buffer.flip();
            int keyLength = buffer.getInt();

            // 读取 key
            buffer = ensureCapacity(buffer, keyLength);
            buffer.clear().limit(keyLength);
            bytesRead = channel.read(buffer);
            if (bytesRead != keyLength) {
                break;
            }
            buffer.flip();
            byte[] key = new byte[keyLength];
            buffer.get(key);

            // 读取 valueLength (4 bytes)
            buffer.clear().limit(4);
            bytesRead = channel.read(buffer);
            if (bytesRead != 4) {
                break;
            }
            buffer.flip();
            int valueLength = buffer.getInt();

            // 读取 value
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

            // 校验 CRC32
            int payloadSize = 1 + 4 + keyLength + 4 + valueLength;
            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize);
            payloadBuf.put(operation);
            payloadBuf.putInt(keyLength);
            payloadBuf.put(key);
            payloadBuf.putInt(valueLength);
            if (value != null) {
                payloadBuf.put(value);
            }
            CRC32 crc = new CRC32();
            crc.update(payloadBuf.array());

            if ((int) crc.getValue() != storedCrc) {
                log.warn("WAL CRC校验失败，截断位置: {}", position);
                break; // CRC 失败，截断后续内容
            }

            position += 4 + payloadSize;
            callback.recoveryEntry(operation, key, value);
        }
    }

    /**
     * 恢复旧格式 WAL（无 CRC32，兼容升级前的文件）。
     * 格式：Op(1B) | KeyLen(4B) | Key | ValLen(4B) | Value
     */
    private void recoverLegacy(FileChannel channel, RecoveryCallback callback) throws IOException {
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
                if (!targetFile.exists()) {
                    log.debug("WAL压缩跳过: 文件已删除 {}", targetFile.getName());
                    return;
                }
                compactWAL(targetFile);
            } catch (IOException e) {
                if (targetFile.exists()) {
                    log.error("WAL压缩失败: {}", targetFile.getName(), e);
                } else {
                    log.debug("WAL压缩跳过: 文件已被清理 {}", targetFile.getName());
                }
            }
        });
        openWAL();
    }

    private void compactWAL(File walFile) throws IOException {
        Map<byte[], WALEntry> operations = new HashMap<>();
        try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            long fileSize = channel.size();

            // 跳过魔数头（旋转文件必然是新格式）
            long position = WAL_HEADER_SIZE;

            while (position < fileSize) {
                // 读取 CRC32 (4 bytes)
                buffer.clear().limit(4);
                int bytesRead = channel.read(buffer);
                if (bytesRead != 4) {
                    break;
                }
                buffer.flip();
                int storedCrc = buffer.getInt();

                // 读取 operation (1 byte)
                buffer.clear().limit(1);
                bytesRead = channel.read(buffer);
                if (bytesRead != 1) {
                    break;
                }
                buffer.flip();
                byte operation = buffer.get();

                // 读取 keyLength (4 bytes)
                buffer.clear().limit(4);
                bytesRead = channel.read(buffer);
                if (bytesRead != 4) {
                    break;
                }
                buffer.flip();
                int keyLength = buffer.getInt();

                // 读取 key
                buffer = ensureCapacity(buffer, keyLength);
                buffer.clear().limit(keyLength);
                bytesRead = channel.read(buffer);
                if (bytesRead != keyLength) {
                    break;
                }
                buffer.flip();
                byte[] key = new byte[keyLength];
                buffer.get(key);

                // 读取 valueLength (4 bytes)
                buffer.clear().limit(4);
                bytesRead = channel.read(buffer);
                if (bytesRead != 4) {
                    break;
                }
                buffer.flip();
                int valueLength = buffer.getInt();

                // 读取 value
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

                // 校验 CRC32
                int payloadSize = 1 + 4 + keyLength + 4 + valueLength;
                ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize);
                payloadBuf.put(operation);
                payloadBuf.putInt(keyLength);
                payloadBuf.put(key);
                payloadBuf.putInt(valueLength);
                if (value != null) {
                    payloadBuf.put(value);
                }
                CRC32 crc = new CRC32();
                crc.update(payloadBuf.array());
                if ((int) crc.getValue() != storedCrc) {
                    log.warn("WAL压缩时CRC校验失败，截断位置: {}", position);
                    break;
                }

                operations.put(key, new WALEntry(operation, key, value));
                position += 4 + payloadSize;
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
            if (walFile.exists()) {
                log.error("写入压缩后的WAL文件失败: {}", walFile.getPath(), e);
            } else {
                log.debug("WAL压缩写入跳过: 文件已被清理 {}", walFile.getName());
            }
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

            // 写入版本魔数头
            ByteBuffer header = ByteBuffer.allocate(WAL_HEADER_SIZE);
            header.putInt(WAL_MAGIC);
            header.flip();
            channel.write(header);
            currentSize = WAL_HEADER_SIZE;
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

        private final byte operation;
        private final byte[] key;
        private final byte[] value;

        public WALEntry(byte operation, byte[] key, byte[] value) {
            this.operation = operation;
            this.key = key;
            this.value = value;
        }

        /**
         * 序列化为带 CRC32 校验的字节缓冲区。
         * 格式：CRC32(4B) | Op(1B) | KeyLen(4B) | Key | ValLen(4B) | Value
         */
        public ByteBuffer serialize() {
            int valueLength = value == null ? 0 : value.length;
            int payloadSize = 1 + 4 + key.length + 4 + valueLength;

            // 先构建 payload 字节数组用于计算 CRC32
            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize);
            payloadBuf.put(operation);
            payloadBuf.putInt(key.length);
            payloadBuf.put(key);
            payloadBuf.putInt(valueLength);
            if (value != null) {
                payloadBuf.put(value);
            }
            byte[] payload = payloadBuf.array();

            CRC32 crc = new CRC32();
            crc.update(payload);

            // 组装完整条目：CRC32(4B) + payload
            ByteBuffer buffer = ByteBuffer.allocate(4 + payloadSize);
            buffer.putInt((int) crc.getValue());
            buffer.put(payload);
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
