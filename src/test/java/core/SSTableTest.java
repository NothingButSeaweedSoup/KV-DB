package core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

class SSTableTest {

    @TempDir
    Path tempDir;

    private Map<byte[], byte[]> createOrderedData(String... keyValues) {
        Map<byte[], byte[]> data = new ConcurrentSkipListMap<>(util.ByteUtil::compare);
        for (int i = 0; i < keyValues.length; i += 2) {
            data.put(keyValues[i].getBytes(), keyValues[i + 1].getBytes());
        }
        return data;
    }

    @Test
    void shouldWriteAndReadAllData() throws IOException {
        Path file = tempDir.resolve("test.sst");
        Map<byte[], byte[]> data = createOrderedData("a", "1", "b", "2", "c", "3");

        try (SSTable sstable = new SSTable(file.toString(), data)) {
            // sstable auto-closes after write
        }

        try (SSTable loaded = new SSTable(file.toString())) {
            Map<byte[], byte[]> all = loaded.getAll();
            assertThat(all).hasSize(3);
            assertThat(all.get("a".getBytes())).containsExactly("1".getBytes());
            assertThat(all.get("b".getBytes())).containsExactly("2".getBytes());
            assertThat(all.get("c".getBytes())).containsExactly("3".getBytes());
        }
    }

    @Test
    void shouldGetSingleKey() throws IOException {
        Path file = tempDir.resolve("single.sst");
        Map<byte[], byte[]> data = createOrderedData("key", "value");

        try (SSTable sstable = new SSTable(file.toString(), data)) {
        }

        try (SSTable loaded = new SSTable(file.toString())) {
            assertThat(loaded.get("key".getBytes())).containsExactly("value".getBytes());
        }
    }

    @Test
    void shouldReturnNullForMissingKey() throws IOException {
        Path file = tempDir.resolve("missing.sst");
        Map<byte[], byte[]> data = createOrderedData("a", "1");

        try (SSTable sstable = new SSTable(file.toString(), data)) {
        }

        try (SSTable loaded = new SSTable(file.toString())) {
            assertThat(loaded.get("z".getBytes())).isNull();
        }
    }

    @Test
    void shouldHandleLargeValue() throws IOException {
        Path file = tempDir.resolve("large.sst");
        byte[] key = "big".getBytes();
        byte[] value = new byte[1024 * 1024];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (i % 256);
        }

        Map<byte[], byte[]> data = new ConcurrentSkipListMap<>(util.ByteUtil::compare);
        data.put(key, value);

        try (SSTable sstable = new SSTable(file.toString(), data)) {
        }

        try (SSTable loaded = new SSTable(file.toString())) {
            assertThat(loaded.get(key)).containsExactly(value);
        }
    }

    @Test
    void shouldRebuildIndexAfterReload() throws IOException {
        Path file = tempDir.resolve("reload.sst");
        Map<byte[], byte[]> data = createOrderedData("x", "10", "y", "20", "z", "30");

        try (SSTable sstable = new SSTable(file.toString(), data)) {
        }

        try (SSTable loaded = new SSTable(file.toString())) {
            assertThat(loaded.get("y".getBytes())).containsExactly("20".getBytes());
            assertThat(loaded.get("x".getBytes())).containsExactly("10".getBytes());
            assertThat(loaded.get("z".getBytes())).containsExactly("30".getBytes());
        }
    }
}
