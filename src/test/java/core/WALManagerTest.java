package core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WALManagerTest {

    @TempDir
    Path tempDir;

    private static class CollectingCallback implements WALManager.RecoveryCallback {
        final List<Entry> entries = new ArrayList<>();

        @Override
        public void recoveryEntry(byte operation, byte[] key, byte[] value) {
            entries.add(new Entry(operation, key, value));
        }
    }

    private record Entry(byte operation, byte[] key, byte[] value) {
    }

    @Test
    void shouldRecoverLoggedEntries() throws IOException {
        Path walDir = tempDir.resolve("wal");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024);
        CollectingCallback callback = new CollectingCallback();
        wal.init(callback);

        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();

        wal.log(Constants.Operation.PUT, key1, value1);
        wal.log(Constants.Operation.DELETE, key2, null);
        wal.close();

        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024);
        CollectingCallback recoveredCallback = new CollectingCallback();
        recovered.init(recoveredCallback);

        assertThat(recoveredCallback.entries).hasSize(2);
        assertThat(recoveredCallback.entries.get(0).operation).isEqualTo(Constants.Operation.PUT);
        assertThat(recoveredCallback.entries.get(0).key).containsExactly(key1);
        assertThat(recoveredCallback.entries.get(0).value).containsExactly(value1);
        assertThat(recoveredCallback.entries.get(1).operation).isEqualTo(Constants.Operation.DELETE);
        assertThat(recoveredCallback.entries.get(1).key).containsExactly(key2);
        assertThat(recoveredCallback.entries.get(1).value).isNull();
    }

    @Test
    void shouldRotateWhenSegmentSizeExceeded() throws IOException {
        Path walDir = tempDir.resolve("wal-rotate");
        WALManager wal = new WALManager(walDir.toString(), 50);
        wal.init(new CollectingCallback());

        for (int i = 0; i < 100; i++) {
            wal.log(Constants.Operation.PUT, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }
        wal.close();

        java.io.File[] walFiles = walDir.toFile().listFiles();
        assertThat(walFiles).isNotNull();
        long walFileCount = java.util.Arrays.stream(walFiles)
                .filter(f -> f.getName().startsWith("WAL"))
                .count();
        assertThat(walFileCount).isGreaterThan(1);
    }

    @Test
    void shouldClearWAL() throws IOException {
        Path walDir = tempDir.resolve("wal-clear");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024);
        wal.init(new CollectingCallback());
        wal.log(Constants.Operation.PUT, "key".getBytes(), "value".getBytes());
        wal.clear();
        wal.close();

        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024);
        CollectingCallback callback = new CollectingCallback();
        recovered.init(callback);
        assertThat(callback.entries).isEmpty();
    }

    @Test
    void shouldStopRecoveryOnCorruptedCRC() throws IOException {
        Path walDir = tempDir.resolve("wal-crc");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024);
        wal.init(new CollectingCallback());

        // 写入 3 条记录
        wal.log(Constants.Operation.PUT, "key1".getBytes(), "value1".getBytes());
        wal.log(Constants.Operation.PUT, "key2".getBytes(), "value2".getBytes());
        wal.log(Constants.Operation.PUT, "key3".getBytes(), "value3".getBytes());
        wal.close();

        // 损坏第二条记录的 CRC（翻转一个字节）
        java.io.File walFile = walDir.resolve("WAL.wal").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
            // 魔数头: 4 字节
            // 第一条记录: 4(CRC) + 1(op) + 4(keyLen) + 4("key1") + 4(valLen) + 6("value1") = 23
            // 第二条记录的 CRC 从偏移量 4 + 23 = 27 开始
            raf.seek(27);
            byte b = raf.readByte();
            raf.seek(27);
            raf.writeByte(b ^ 0xFF); // 翻转一个字节
        }

        // 恢复时应在损坏处截断，只恢复第一条记录
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024);
        CollectingCallback callback = new CollectingCallback();
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(1);
        assertThat(callback.entries.get(0).key).containsExactly("key1".getBytes());
    }

    @Test
    void shouldRecoverLegacyFormatWAL() throws IOException {
        Path walDir = tempDir.resolve("wal-legacy");
        java.io.File walFile = walDir.resolve("WAL.wal").toFile();
        walDir.toFile().mkdirs();

        // 手动写入旧格式 WAL 文件（无魔数头，无 CRC）
        // 格式：Op(1B) | KeyLen(4B) | Key | ValLen(4B) | Value
        try (RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
            // Entry 1: PUT "leg1" -> "val1"
            raf.writeByte(Constants.Operation.PUT);
            raf.writeInt(4);
            raf.write("leg1".getBytes());
            raf.writeInt(4);
            raf.write("val1".getBytes());

            // Entry 2: DELETE "leg2"
            raf.writeByte(Constants.Operation.DELETE);
            raf.writeInt(4);
            raf.write("leg2".getBytes());
            raf.writeInt(0);
        }

        // 恢复应使用兼容模式读取旧格式
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024);
        CollectingCallback callback = new CollectingCallback();
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(2);
        assertThat(callback.entries.get(0).operation).isEqualTo(Constants.Operation.PUT);
        assertThat(callback.entries.get(0).key).containsExactly("leg1".getBytes());
        assertThat(callback.entries.get(0).value).containsExactly("val1".getBytes());
        assertThat(callback.entries.get(1).operation).isEqualTo(Constants.Operation.DELETE);
        assertThat(callback.entries.get(1).key).containsExactly("leg2".getBytes());
        assertThat(callback.entries.get(1).value).isNull();
    }
}
