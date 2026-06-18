package core;

import config.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试：验证崩溃恢复、数据一致性等端到端场景。
 * <p>
 * 使用 SYNC fsync 策略确保 WAL 数据立即落盘，不依赖 OS 页缓存。
 */
class IntegrationTest {

    @TempDir
    Path tempDir;

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private String val(Object obj) {
        return obj == null ? null : obj.toString();
    }

    /**
     * 创建使用 SYNC fsync 策略的引擎，确保每次写入立即落盘。
     */
    private LSMStorageEngine createSyncEngine(Path dataDir) throws IOException {
        Config config = new Config.Builder()
                .setDataDir(dataDir.toString())
                .setFsyncStrategy(core.FsyncStrategy.SYNC)
                .build();
        return new LSMStorageEngine(config);
    }

    @Test
    void shouldRecoverAfterCrash() throws IOException {
        Path dataDir = tempDir.resolve("crash-recovery");

        // 1. 写入数据但不 flush（数据只在 WAL 和 MemTable 中）
        LSMStorageEngine engine = createSyncEngine(dataDir);
        for (int i = 0; i < 50; i++) {
            engine.put(key("crash-key-" + i), "crash-val-" + i);
        }
        // 2. 模拟崩溃：SYNC 策略保证 WAL 已落盘，直接丢弃引用

        // 3. 重新打开引擎，验证 WAL 恢复
        LSMStorageEngine recovered = createSyncEngine(dataDir);
        for (int i = 0; i < 50; i++) {
            assertThat(val(recovered.get(key("crash-key-" + i))))
                    .isEqualTo("crash-val-" + i);
        }
        recovered.close();
    }

    @Test
    void shouldRecoverAfterFlushAndCrash() throws IOException {
        Path dataDir = tempDir.resolve("flush-crash");

        // 1. 写入数据并 flushAndWait，确保 SSTable 落盘
        LSMStorageEngine engine = createSyncEngine(dataDir);
        for (int i = 0; i < 100; i++) {
            engine.put(key("flush-key-" + i), "flush-val-" + i);
        }
        engine.flushAndWait();

        // 2. 再写入新数据（不 flush，依赖 WAL）
        for (int i = 100; i < 150; i++) {
            engine.put(key("flush-key-" + i), "flush-val-" + i);
        }
        // 3. 模拟崩溃

        // 4. 重新打开，验证所有数据
        LSMStorageEngine recovered = createSyncEngine(dataDir);
        for (int i = 0; i < 150; i++) {
            assertThat(val(recovered.get(key("flush-key-" + i))))
                    .isEqualTo("flush-val-" + i);
        }
        recovered.close();
    }

    @Test
    void shouldHandleDeleteRecovery() throws IOException {
        Path dataDir = tempDir.resolve("delete-recovery");

        LSMStorageEngine engine = createSyncEngine(dataDir);
        engine.put(key("to-keep"), "keep-val");
        engine.put(key("to-delete"), "delete-val");
        engine.delete(key("to-delete"));
        // 不关闭，模拟崩溃

        LSMStorageEngine recovered = createSyncEngine(dataDir);
        assertThat(val(recovered.get(key("to-keep")))).isEqualTo("keep-val");
        assertThat(recovered.get(key("to-delete"))).isNull();
        recovered.close();
    }

    @Test
    void shouldRecoverLargeDataset() throws IOException {
        Path dataDir = tempDir.resolve("large-recovery");

        LSMStorageEngine engine = createSyncEngine(dataDir);
        int count = 500;
        for (int i = 0; i < count; i++) {
            engine.put(key("large-" + i), "value-" + i);
        }
        // 模拟崩溃

        LSMStorageEngine recovered = createSyncEngine(dataDir);
        for (int i = 0; i < count; i++) {
            assertThat(val(recovered.get(key("large-" + i))))
                    .isEqualTo("value-" + i);
        }
        recovered.close();
    }

    @Test
    void shouldRecoverWithMultipleFlushes() throws IOException {
        Path dataDir = tempDir.resolve("multi-flush");

        LSMStorageEngine engine = createSyncEngine(dataDir);
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < 50; i++) {
                engine.put(key("r" + round + "-k" + i), "r" + round + "-v" + i);
            }
            engine.flushAndWait();
        }
        // 最后一轮不 flush，依赖 WAL 恢复
        for (int i = 50; i < 100; i++) {
            engine.put(key("r2-k" + i), "r2-v" + i);
        }
        // 模拟崩溃

        LSMStorageEngine recovered = createSyncEngine(dataDir);
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < 50; i++) {
                assertThat(val(recovered.get(key("r" + round + "-k" + i))))
                        .isEqualTo("r" + round + "-v" + i);
            }
        }
        // 最后一轮的全部数据
        for (int i = 0; i < 100; i++) {
            assertThat(val(recovered.get(key("r2-k" + i))))
                    .isEqualTo("r2-v" + i);
        }
        recovered.close();
    }
}
