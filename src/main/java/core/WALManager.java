package core;

import util.ByteUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WALManager {

    private final String walPath;
    private final long segmentSize;
    private FileChannel channel;
    private long currentSize;
    private final ExecutorService compressor;

    public WALManager(String walPath, long segmentSize) throws IOException {
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("WAL段大小必须大于0");
        }

        this.segmentSize = segmentSize;
        this.walPath = walPath + Constants.System.FILE_SEPARATOR + "WAL" + Constants.File.WAL_EXTENSION;

        File walDir = new File(walPath);
        if (!walDir.exists() && !walDir.mkdirs()) {
            throw new IOException("创建WAL目录失败");
        }
        this.compressor = Executors.newFixedThreadPool(2);
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

    public void log(byte operation, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        WALEntry entry = new WALEntry(operation, key, value);
        ByteBuffer buffer = entry.serialize();

        if (!buffer.hasRemaining()) {
            buffer.flip();
        }

        int bytesWritten = channel.write(buffer);
        currentSize += bytesWritten;

        if (currentSize + buffer.remaining() > segmentSize) {
            rotateWAL();
        }

        channel.force(false);
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
        if (!currentWAL.renameTo(new File(newName))) {
            throw new IOException("无法重命名WAL文件");
        }
        compressor.submit(() -> {
            try {
                compactWAL(new File(newName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        openWAL();
    }

    private void compactWAL(File walFile) throws IOException {
//        Map<byte[], WALEntry> operations = new TreeMap<>();
        TreeMap<byte[], WALEntry> operations = new TreeMap<>(ByteUtil::compare);
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

                if (operation == Constants.Operation.PUT) {
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
                } else if (operation == Constants.Operation.DELETE) {
                    operations.put(key, new WALEntry(operation, key, null));
                    position += 4;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (WALEntry entry : operations.values()) {
                channel.write(entry.serialize());
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void clear() throws IOException {
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

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    public interface RecoveryCallback {
        void recoveryEntry(byte operation, byte[] key, byte[] value) throws IOException;
    }

    private class WALEntry {
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
            ByteBuffer buffer;
            if (value == null) {
                buffer = ByteBuffer.allocate(LOG_ENTRY_SIZE + key.length);
            } else {
                buffer = ByteBuffer.allocate(LOG_ENTRY_SIZE + key.length + value.length);
            }


            buffer.put(operation);
            buffer.putInt(key.length);
            buffer.put(key);
            if (value != null) {
                buffer.putInt(value.length);
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
    }
}
