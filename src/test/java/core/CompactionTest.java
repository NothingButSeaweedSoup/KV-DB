package core;

import config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.ByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compaction 单元测试。
 * 覆盖层级阈值触发、key 范围拆分、跨层重叠合并、原子元数据替换等场景。
 */
class CompactionTest {

    @TempDir
    Path tempDir;

    private Config config;
    private VersionSet versionSet;
    private Compaction compaction;
    private final AtomicInteger sstableCounter = new AtomicInteger(0);

    @BeforeEach
    void setUp() throws IOException {
        config = new Config.Builder()
                .setDataDir(tempDir.toString())
                .setWalSegmentSize(1024 * 1024)
                .setMemTableThreshold(1024 * 1024)
                .setFsyncStrategy(FsyncStrategy.BATCH)
                .setSstTargetFileSize(1024) // 1KB，便于测试拆分
                .setLevel0FileNumCompactionTrigger(3) // 3 个文件触发
                .build();
        versionSet = new VersionSet(tempDir.toString(), 7);
        compaction = new Compaction(config, versionSet);
    }

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] val(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 创建一个 SSTable 文件并注册到 VersionSet。
     */
    private void createSSTable(int level, Map<byte[], byte[]> data) throws IOException {
        Path levelDir = tempDir.resolve("level-" + level);
        Files.createDirectories(levelDir);
        String path = levelDir.resolve("sstable-" + sstableCounter.incrementAndGet() + "-" + System.nanoTime() + ".sst").toString();
        SSTable sstable = new SSTable(path, data);
        byte[] minKey = sstable.getMinKey();
        byte[] maxKey = sstable.getMaxKey();
        int entries = sstable.getEntryCount();
        sstable.close();
        versionSet.addSSTable(level, new VersionSet.SSTableMeta(path, minKey, maxKey, entries));
    }

    /**
     * 构建有序数据 map。
     */
    private ConcurrentSkipListMap<byte[], byte[]> sortedData(String... kvPairs) {
        ConcurrentSkipListMap<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
        for (int i = 0; i < kvPairs.length; i += 2) {
            data.put(key(kvPairs[i]), val(kvPairs[i + 1]));
        }
        return data;
    }

    @Test
    void shouldNotTriggerWhenBelowThreshold() throws IOException {
        // level0FileNumCompactionTrigger = 3, 只放 2 个文件
        createSSTable(0, sortedData("a", "1", "b", "2"));
        createSSTable(0, sortedData("c", "3", "d", "4"));

        assertThat(versionSet.getLevelSize(0)).isEqualTo(2);

        compaction.checkAndCompact();

        // 不应触发 compaction
        assertThat(versionSet.getLevelSize(0)).isEqualTo(2);
    }

    @Test
    void shouldTriggerCompactionWhenThresholdReached() throws IOException {
        // 3 个文件达到阈值
        createSSTable(0, sortedData("a", "1", "b", "2"));
        createSSTable(0, sortedData("c", "3", "d", "4"));
        createSSTable(0, sortedData("e", "5", "f", "6"));

        assertThat(versionSet.getLevelSize(0)).isEqualTo(3);

        compaction.checkAndCompact();

        // Level 0 的文件应被移走（合并到 level 1）
        assertThat(versionSet.getLevelSize(0)).isZero();
        // Level 1 应有新生成的 SSTable
        assertThat(versionSet.getLevelSize(1)).isPositive();
    }

    @Test
    void shouldMergeOverlappingKeysAcrossSSTables() throws IOException {
        // 两个 SSTable 有重叠 key
        createSSTable(0, sortedData("a", "1", "c", "3"));
        createSSTable(0, sortedData("b", "2", "d", "4"));
        createSSTable(0, sortedData("a", "10", "e", "5")); // "a" 被覆盖

        compaction.checkAndCompact();

        // 读取 level 1 的数据验证合并结果
        List<VersionSet.SSTableMeta> level1 = versionSet.getLevel(1);
        assertThat(level1).isNotEmpty();

        // 通过 SSTable 读取验证 key 覆盖正确
        boolean foundCorrectA = false;
        for (VersionSet.SSTableMeta meta : level1) {
            SSTable sstable = new SSTable(meta.filePath());
            byte[] aValue = sstable.get(key("a"));
            if (aValue != null) {
                // "a" 的最新值应该是 "10"（最后写入的）
                assertThat(new String(aValue, StandardCharsets.UTF_8)).isEqualTo("10");
                foundCorrectA = true;
            }
            sstable.close();
        }
        assertThat(foundCorrectA).isTrue();
    }

    @Test
    void shouldSplitLargeOutputIntoMultipleSSTables() throws IOException {
        // sstTargetFileSize = 1024 (1KB)，插入足够多数据触发拆分
        ConcurrentSkipListMap<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
        for (int i = 0; i < 100; i++) {
            String k = String.format("key-%04d", i);
            String v = "value-" + i + "-padding-padding-padding"; // ~35 bytes per entry
            data.put(key(k), val(v));
        }
        createSSTable(0, data);

        // 再加两个小文件达到阈值
        createSSTable(0, sortedData("zzz", "end"));
        createSSTable(0, sortedData("aaa", "start"));

        compaction.checkAndCompact();

        // Level 1 应有多个 SSTable（因为数据量超过 targetFileSize）
        assertThat(versionSet.getLevelSize(1)).isGreaterThanOrEqualTo(2);
    }

    @Test
    void shouldPreserveKeyRangeIntegrity() throws IOException {
        // 验证 compaction 后同层 SSTable key 范围不重叠（Level ≥1）
        createSSTable(0, sortedData("m", "1", "n", "2", "o", "3"));
        createSSTable(0, sortedData("a", "1", "b", "2", "c", "3"));
        createSSTable(0, sortedData("x", "1", "y", "2", "z", "3"));

        compaction.checkAndCompact();

        // Level 1 的 SSTable key 范围不应重叠
        List<VersionSet.SSTableMeta> level1 = versionSet.getLevel(1);
        if (level1.size() > 1) {
            for (int i = 0; i < level1.size(); i++) {
                for (int j = i + 1; j < level1.size(); j++) {
                    VersionSet.SSTableMeta a = level1.get(i);
                    VersionSet.SSTableMeta b = level1.get(j);
                    // a.maxKey < b.minKey 或 b.maxKey < a.minKey（不重叠）
                    boolean noOverlap = ByteUtil.compare(a.maxKey(), b.minKey()) < 0
                            || ByteUtil.compare(b.maxKey(), a.minKey()) < 0;
                    assertThat(noOverlap)
                            .as("SSTable key ranges should not overlap at level 1")
                            .isTrue();
                }
            }
        }
    }

    @Test
    void shouldDeleteOldSSTableFiles() throws IOException {
        createSSTable(0, sortedData("a", "1"));
        createSSTable(0, sortedData("b", "2"));
        createSSTable(0, sortedData("c", "3"));

        // 记录旧文件路径
        List<String> oldPaths = versionSet.getLevel(0).stream()
                .map(VersionSet.SSTableMeta::filePath)
                .toList();

        compaction.checkAndCompact();

        // 旧文件应被删除
        for (String path : oldPaths) {
            assertThat(java.nio.file.Path.of(path).toFile().exists())
                    .as("Old SSTable file should be deleted: %s", path)
                    .isFalse();
        }
    }

    @Test
    void shouldRunCompactionAsynchronously() throws Exception {
        createSSTable(0, sortedData("a", "1"));
        createSSTable(0, sortedData("b", "2"));
        createSSTable(0, sortedData("c", "3"));

        // 异步触发
        compaction.checkAndCompactAsync();

        // 等待异步完成（最多 5 秒）
        long deadline = System.currentTimeMillis() + 5000;
        while (versionSet.getLevelSize(0) > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertThat(versionSet.getLevelSize(0)).isZero();
    }

    @Test
    void shouldCompactAcrossMultipleLevels() throws IOException {
        // 先在 level 0 写入足够数据触发 level 0 -> level 1 compaction
        for (int i = 0; i < 5; i++) {
            ConcurrentSkipListMap<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
            for (int j = 0; j < 10; j++) {
                String k = String.format("batch%d-key%04d", i, j);
                data.put(key(k), val("v" + j));
            }
            createSSTable(0, data);
        }

        compaction.checkAndCompact();

        // 验证 level 0 已清空，数据在 level 1
        assertThat(versionSet.getLevelSize(0)).isZero();
        assertThat(versionSet.getLevelSize(1)).isPositive();

        // 通过 SSTable 读取验证部分 key 存在（验证数据完整性）
        String[] sampleKeys = {"batch0-key0000", "batch2-key0005", "batch4-key0009"};
        for (String k : sampleKeys) {
            boolean found = false;
            for (VersionSet.SSTableMeta meta : versionSet.getLevel(1)) {
                SSTable sstable = new SSTable(meta.filePath());
                byte[] value = sstable.get(key(k));
                sstable.close();
                if (value != null) {
                    found = true;
                    break;
                }
            }
            assertThat(found).as("Key %s should be found in level 1", k).isTrue();
        }
    }

    @Test
    void shouldCloseCleanly() {
        // close() 不应抛异常
        compaction.close();
    }
}
