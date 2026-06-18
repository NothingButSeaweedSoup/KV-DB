package core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.ByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotManagerTest {

    @TempDir
    Path tempDir;

    private VersionSet versionSet;
    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() throws IOException {
        versionSet = new VersionSet(tempDir.toString(), 7);
        snapshotManager = new SnapshotManager(tempDir.toString(), versionSet);
    }

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] val(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private void createSSTable(int level, String... kvPairs) throws IOException {
        ConcurrentSkipListMap<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
        for (int i = 0; i < kvPairs.length; i += 2) {
            data.put(key(kvPairs[i]), val(kvPairs[i + 1]));
        }
        Path levelDir = tempDir.resolve("level-" + level);
        Files.createDirectories(levelDir);
        String path = levelDir.resolve("sstable-" + System.nanoTime() + ".sst").toString();
        SSTable sst = new SSTable(path, data);
        byte[] minKey = sst.getMinKey();
        byte[] maxKey = sst.getMaxKey();
        int entries = sst.getEntryCount();
        sst.close();
        versionSet.addSSTable(level, new VersionSet.SSTableMeta(path, minKey, maxKey, entries));
    }

    @Test
    void shouldCreateSnapshot() throws IOException {
        createSSTable(0, "a", "1", "b", "2");
        createSSTable(0, "c", "3");
        createSSTable(1, "d", "4");

        SnapshotManager.SnapshotInfo info = snapshotManager.createSnapshot();

        assertThat(info.totalFiles()).isEqualTo(3);
        assertThat(info.path()).contains("snapshot-");
        assertThat(info.files()).hasSize(3);

        // 验证快照目录存在
        Path snapshotPath = Path.of(info.path());
        assertThat(snapshotPath).exists();
        assertThat(snapshotPath.resolve("snapshot-info.txt")).exists();

        // 验证文件已复制
        assertThat(snapshotPath.resolve("level-0")).exists();
        assertThat(snapshotPath.resolve("level-1")).exists();
    }

    @Test
    void shouldCreateEmptySnapshot() throws IOException {
        SnapshotManager.SnapshotInfo info = snapshotManager.createSnapshot();

        assertThat(info.totalFiles()).isZero();
        assertThat(info.totalBytes()).isZero();
    }

    @Test
    void shouldListSnapshots() throws IOException {
        assertThat(snapshotManager.listSnapshots()).isEmpty();

        createSSTable(0, "a", "1");
        snapshotManager.createSnapshot();

        List<String> snapshots = snapshotManager.listSnapshots();
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots.get(0)).startsWith("snapshot-");
    }

    @Test
    void shouldCreateMultipleSnapshots() throws IOException, InterruptedException {
        createSSTable(0, "a", "1");

        snapshotManager.createSnapshot();
        Thread.sleep(1100); // 确保时间戳不同
        snapshotManager.createSnapshot();

        assertThat(snapshotManager.listSnapshots()).hasSize(2);
    }

    @Test
    void shouldPreserveDataInSnapshot() throws IOException {
        createSSTable(0, "key1", "value1", "key2", "value2");

        SnapshotManager.SnapshotInfo info = snapshotManager.createSnapshot();

        // 通过快照中的文件读取数据
        Path snapshotPath = Path.of(info.path());
        Path level0Dir = snapshotPath.resolve("level-0");
        Files.list(level0Dir)
                .filter(p -> p.toString().endsWith(".sst"))
                .forEach(sstFile -> {
                    try {
                        SSTable sst = new SSTable(sstFile.toString());
                        assertThat(sst.getEntryCount()).isPositive();
                        sst.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void incrementalBackupShouldCopyNewFiles() throws IOException {
        createSSTable(0, "a", "1");

        List<String> backedUp = snapshotManager.incrementalBackup("20200101-000000");
        assertThat(backedUp).hasSize(1);
    }
}
