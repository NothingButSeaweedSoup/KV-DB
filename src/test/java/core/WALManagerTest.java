package core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
}
