package core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class LSMStorageEngineTest {

    @TempDir
    Path tempDir;

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private String value(Object obj) {
        return obj == null ? null : obj.toString();
    }

    @Test
    void shouldPutAndGet() throws IOException {
        Path dataDir = tempDir.resolve("db1");
        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            engine.put(key("hello"), "world");
            assertThat(value(engine.get(key("hello")))).isEqualTo("world");
        }
    }

    @Test
    void shouldDeleteKey() throws IOException {
        Path dataDir = tempDir.resolve("db2");
        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            engine.put(key("hello"), "world");
            engine.delete(key("hello"));
            assertThat(engine.get(key("hello"))).isNull();
        }
    }

    @Test
    void shouldFlushAndReload() throws IOException {
        Path dataDir = tempDir.resolve("db3");
        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            engine.put(key("k1"), "v1");
            engine.put(key("k2"), "v2");
            engine.flush();
        }

        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            assertThat(value(engine.get(key("k1")))).isEqualTo("v1");
            assertThat(value(engine.get(key("k2")))).isEqualTo("v2");
        }
    }

    @Test
    void shouldRecoverUnflushedDataFromWAL() throws IOException {
        Path dataDir = tempDir.resolve("db4");
        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            for (int i = 0; i < 100; i++) {
                engine.put(key("key-" + i), "value-" + i);
            }
        }

        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            for (int i = 0; i < 100; i++) {
                assertThat(value(engine.get(key("key-" + i)))).isEqualTo("value-" + i);
            }
        }
    }

    @Test
    void shouldHandleMultipleFlushesAndCompaction() throws IOException {
        Path dataDir = tempDir.resolve("db5");
        try (LSMStorageEngine engine = new LSMStorageEngine(dataDir.toString())) {
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 100; i++) {
                    engine.put(key("round" + round + "-key" + i), "value" + i);
                }
                engine.flush();
            }

            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 100; i++) {
                    assertThat(value(engine.get(key("round" + round + "-key" + i)))).isEqualTo("value" + i);
                }
            }
        }
    }
}
