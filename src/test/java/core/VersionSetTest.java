package core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

class VersionSetTest {

    @TempDir
    Path tempDir;

    private Map<byte[], byte[]> orderedData(String... keyValues) {
        Map<byte[], byte[]> data = new ConcurrentSkipListMap<>(util.ByteUtil::compare);
        for (int i = 0; i < keyValues.length; i += 2) {
            data.put(keyValues[i].getBytes(), keyValues[i + 1].getBytes());
        }
        return data;
    }

    @Test
    void loadFromDiskAndFindCandidates() throws IOException {
        Path dbDir = tempDir.resolve("db1");
        Path level0 = dbDir.resolve("level-0");
        level0.toFile().mkdirs();

        // 创建一个 SSTable
        String sstPath = level0.resolve("test.sst").toString();
        Map<byte[], byte[]> data = orderedData("a", "1", "b", "2", "c", "3");
        SSTable sst = new SSTable(sstPath, data);
        sst.close();

        VersionSet vs = new VersionSet(dbDir.toString(), 7);
        vs.loadFromDisk();

        assertThat(vs.getLevelSize(0)).isEqualTo(1);

        // key "b" 应该在候选列表中
        assertThat(vs.findCandidates("b".getBytes())).hasSize(1);
    }

    @Test
    void addAndRemoveSSTables() throws IOException {
        VersionSet vs = new VersionSet(tempDir.resolve("db2").toString(), 7);

        VersionSet.SSTableMeta meta1 = new VersionSet.SSTableMeta(
                "path1.sst", "a".getBytes(), "m".getBytes(), 10);
        VersionSet.SSTableMeta meta2 = new VersionSet.SSTableMeta(
                "path2.sst", "n".getBytes(), "z".getBytes(), 10);

        vs.addSSTable(0, meta1);
        vs.addSSTable(0, meta2);
        assertThat(vs.getLevelSize(0)).isEqualTo(2);

        // 替换
        vs.replaceSSTables(0, java.util.List.of(meta1, meta2), java.util.List.of());
        assertThat(vs.getLevelSize(0)).isEqualTo(0);
    }

    @Test
    void level0ReturnsAllCandidates() throws IOException {
        VersionSet vs = new VersionSet(tempDir.resolve("db3").toString(), 7);

        // Level 0 的文件 key 可重叠
        vs.addSSTable(0, new VersionSet.SSTableMeta("f1.sst", "a".getBytes(), "z".getBytes(), 10));
        vs.addSSTable(0, new VersionSet.SSTableMeta("f2.sst", "a".getBytes(), "z".getBytes(), 10));

        // Level 0 应返回全部（不按 key 范围过滤）
        assertThat(vs.findCandidates("m".getBytes())).hasSize(2);
    }

    @Test
    void level1FiltersByKeyRange() throws IOException {
        VersionSet vs = new VersionSet(tempDir.resolve("db4").toString(), 7);

        vs.addSSTable(1, new VersionSet.SSTableMeta("f1.sst", "a".getBytes(), "f".getBytes(), 5));
        vs.addSSTable(1, new VersionSet.SSTableMeta("f2.sst", "g".getBytes(), "m".getBytes(), 5));
        vs.addSSTable(1, new VersionSet.SSTableMeta("f3.sst", "n".getBytes(), "z".getBytes(), 5));

        // "c" 只匹配 f1
        assertThat(vs.findCandidates("c".getBytes())).hasSize(1);
        // "j" 只匹配 f2
        assertThat(vs.findCandidates("j".getBytes())).hasSize(1);
        // "x" 只匹配 f3
        assertThat(vs.findCandidates("x".getBytes())).hasSize(1);
    }

    @Test
    void mightContainFiltersCorrectly() {
        VersionSet.SSTableMeta meta = new VersionSet.SSTableMeta(
                "test.sst", "b".getBytes(), "y".getBytes(), 10);

        assertThat(meta.mightContain("a".getBytes())).isFalse();
        assertThat(meta.mightContain("b".getBytes())).isTrue();
        assertThat(meta.mightContain("m".getBytes())).isTrue();
        assertThat(meta.mightContain("y".getBytes())).isTrue();
        assertThat(meta.mightContain("z".getBytes())).isFalse();
    }

    @Test
    void clearLevel() throws IOException {
        VersionSet vs = new VersionSet(tempDir.resolve("db5").toString(), 7);
        vs.addSSTable(0, new VersionSet.SSTableMeta("f1.sst", "a".getBytes(), "z".getBytes(), 10));
        vs.addSSTable(1, new VersionSet.SSTableMeta("f2.sst", "a".getBytes(), "z".getBytes(), 10));

        vs.clearLevel(0);
        assertThat(vs.getLevelSize(0)).isEqualTo(0);
        assertThat(vs.getLevelSize(1)).isEqualTo(1);
    }

    @Test
    void loadFromDiskWithMultipleFiles() throws IOException {
        Path dbDir = tempDir.resolve("db6");
        Path level0 = dbDir.resolve("level-0");
        level0.toFile().mkdirs();

        // 创建多个 SSTable
        for (int i = 0; i < 3; i++) {
            String sstPath = level0.resolve("test" + i + ".sst").toString();
            Map<byte[], byte[]> data = orderedData(
                    "key" + i + "-a", "val" + i,
                    "key" + i + "-b", "val" + i);
            SSTable sst = new SSTable(sstPath, data);
            sst.close();
        }

        VersionSet vs = new VersionSet(dbDir.toString(), 7);
        vs.loadFromDisk();
        assertThat(vs.getLevelSize(0)).isEqualTo(3);
    }
}
