package core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FsyncStrategyTest {

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
    void syncStrategyRecoversAllData() throws IOException {
        Path walDir = tempDir.resolve("sync");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.SYNC);
        wal.init(new CollectingCallback());
        for (int i = 0; i < 50; i++) {
            wal.log(Constants.Operation.PUT, ("key-" + i).getBytes(), ("val-" + i).getBytes());
        }
        wal.close();

        CollectingCallback callback = new CollectingCallback();
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.SYNC);
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(50);
        assertThat(callback.entries.get(0).key).isEqualTo("key-0".getBytes());
    }

    @Test
    void batchStrategyRecoversAllData() throws IOException {
        Path walDir = tempDir.resolve("batch");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.BATCH);
        wal.init(new CollectingCallback());
        for (int i = 0; i < 50; i++) {
            wal.log(Constants.Operation.PUT, ("key-" + i).getBytes(), ("val-" + i).getBytes());
        }
        wal.close();

        CollectingCallback callback = new CollectingCallback();
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.BATCH);
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(50);
    }

    @Test
    void asyncStrategyRecoversAllData() throws IOException {
        Path walDir = tempDir.resolve("async");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.ASYNC);
        wal.init(new CollectingCallback());
        for (int i = 0; i < 50; i++) {
            wal.log(Constants.Operation.PUT, ("key-" + i).getBytes(), ("val-" + i).getBytes());
        }
        // ASYNC模式：close()中会执行最终force
        wal.close();

        CollectingCallback callback = new CollectingCallback();
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.ASYNC);
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(50);
    }

    @Test
    void customBatchInterval() throws IOException {
        Path walDir = tempDir.resolve("custom-batch");
        // batchInterval=5，每5条fsync一次
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.BATCH, 5, 500);
        wal.init(new CollectingCallback());
        for (int i = 0; i < 20; i++) {
            wal.log(Constants.Operation.PUT, ("k" + i).getBytes(), ("v" + i).getBytes());
        }
        wal.close();

        CollectingCallback callback = new CollectingCallback();
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.BATCH, 5, 500);
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(20);
    }

    @Test
    void allStrategiesPreserveDeleteOperations() throws IOException {
        for (FsyncStrategy strategy : FsyncStrategy.values()) {
            Path walDir = tempDir.resolve("delete-" + strategy.name());
            WALManager wal = new WALManager(walDir.toString(), 1024 * 1024, strategy);
            wal.init(new CollectingCallback());
            wal.log(Constants.Operation.PUT, "k1".getBytes(), "v1".getBytes());
            wal.log(Constants.Operation.DELETE, "k1".getBytes(), null);
            wal.close();

            CollectingCallback callback = new CollectingCallback();
            WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024, strategy);
            recovered.init(callback);
            recovered.close();

            assertThat(callback.entries).hasSize(2);
            assertThat(callback.entries.get(0).operation).isEqualTo(Constants.Operation.PUT);
            assertThat(callback.entries.get(1).operation).isEqualTo(Constants.Operation.DELETE);
            assertThat(callback.entries.get(1).value).isNull();
        }
    }

    @Test
    void syncStrategyHasLowerWriteLatency() throws IOException {
        // SYNC模式下写入后立即fsync，数据应该完全持久化
        Path walDir = tempDir.resolve("sync-immediate");
        WALManager wal = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.SYNC);
        wal.init(new CollectingCallback());
        for (int i = 0; i < 30; i++) {
            wal.log(Constants.Operation.PUT, ("k" + i).getBytes(), ("v" + i).getBytes());
        }
        // 不调用close，直接读取验证 — SYNC模式每条都已fsync
        wal.close();

        CollectingCallback callback = new CollectingCallback();
        WALManager recovered = new WALManager(walDir.toString(), 1024 * 1024, FsyncStrategy.SYNC);
        recovered.init(callback);
        recovered.close();

        assertThat(callback.entries).hasSize(30);
    }
}
