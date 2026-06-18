package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * SSTable 元数据管理器。
 * <p>
 * 维护每层 SSTable 的元数据（文件路径、key 范围），提供按 key 范围快速过滤的能力，
 * 避免每次查询都打开全部 SSTable 文件。
 */
public class VersionSet {

    private static final Logger log = LoggerFactory.getLogger(VersionSet.class);

    private final String dbDir;
    private final int maxLevel;
    private final List<CopyOnWriteArrayList<SSTableMeta>> levels;

    public VersionSet(String dbDir, int maxLevel) {
        this.dbDir = dbDir;
        this.maxLevel = maxLevel;
        this.levels = new ArrayList<>(maxLevel);
        for (int i = 0; i < maxLevel; i++) {
            levels.add(new CopyOnWriteArrayList<>());
        }
    }

    /**
     * 扫描磁盘目录加载所有 SSTable 元数据。
     */
    public void loadFromDisk() throws IOException {
        for (int level = 0; level < maxLevel; level++) {
            Path levelPath = Path.of(dbDir, "level-" + level);
            if (!levelPath.toFile().exists()) {
                continue;
            }
            File[] files = levelPath.toFile().listFiles(
                    (dir, name) -> name.endsWith(Constants.File.SST_EXTENSION));
            if (files == null) {
                continue;
            }
            for (File file : files) {
                try {
                    SSTableMeta meta = SSTableMeta.fromFile(file.getAbsolutePath());
                    levels.get(level).add(meta);
                } catch (IOException e) {
                    log.warn("加载SSTable元数据失败: {}", file.getPath(), e);
                }
            }
        }
    }

    /**
     * 查找可能包含指定 key 的 SSTable 元数据列表。
     * <p>
     * Level 0：文件间 key 可重叠，全部返回。
     * Level ≥1：同层 key 不重叠，利用排序跳过不匹配的文件。
     *
     * @param key 查询的 key
     * @return 候选 SSTable 元数据列表，按层级从新到旧排列
     */
    public List<SSTableMeta> findCandidates(byte[] key) {
        List<SSTableMeta> candidates = new ArrayList<>();
        for (int level = 0; level < maxLevel; level++) {
            List<SSTableMeta> levelMetas = levels.get(level);
            if (levelMetas.isEmpty()) {
                continue;
            }
            if (level == 0) {
                // Level 0：key 可重叠，全部返回
                candidates.addAll(levelMetas);
            } else {
                // Level ≥1：key 不重叠，二分查找过滤
                for (SSTableMeta meta : levelMetas) {
                    if (meta.mightContain(key)) {
                        candidates.add(meta);
                    }
                }
            }
        }
        return candidates;
    }

    /**
     * 获取指定层级的 SSTable 元数据列表。
     */
    public List<SSTableMeta> getLevel(int level) {
        return Collections.unmodifiableList(levels.get(level));
    }

    /**
     * 获取最大层级数。
     */
    public int getMaxLevel() {
        return maxLevel;
    }

    /**
     * 获取指定层级的 SSTable 数量。
     */
    public int getLevelSize(int level) {
        return levels.get(level).size();
    }

    /**
     * 在指定层级注册新的 SSTable（flush 或 compaction 完成后调用）。
     */
    public void addSSTable(int level, SSTableMeta meta) {
        if (level < 0 || level >= maxLevel) {
            throw new IllegalArgumentException("层级越界: " + level);
        }
        levels.get(level).add(meta);
    }

    /**
     * 批量替换指定层级的 SSTable（compaction 完成后原子替换）。
     *
     * @param level    目标层级
     * @param toRemove 要移除的 SSTable 元数据
     * @param toAdd    要添加的 SSTable 元数据
     */
    public void replaceSSTables(int level, Collection<SSTableMeta> toRemove, Collection<SSTableMeta> toAdd) {
        CopyOnWriteArrayList<SSTableMeta> levelList = levels.get(level);
        levelList.removeAll(toRemove);
        levelList.addAll(toAdd);
    }

    /**
     * 移除指定层级的所有 SSTable 元数据。
     */
    public void clearLevel(int level) {
        levels.get(level).clear();
    }

    /**
     * SSTable 元数据。
     *
     * @param filePath 文件路径
     * @param minKey   最小 key（含）
     * @param maxKey   最大 key（含）
     * @param entries  条目数
     */
    public record SSTableMeta(String filePath, byte[] minKey, byte[] maxKey, int entries) {

        /**
         * 从 SSTable 文件加载元数据。
         */
        public static SSTableMeta fromFile(String filePath) throws IOException {
            try (SSTable sstable = new SSTable(filePath)) {
                return new SSTableMeta(
                        filePath,
                        sstable.getMinKey(),
                        sstable.getMaxKey(),
                        sstable.getEntryCount()
                );
            }
        }

        /**
         * 判断此 SSTable 是否可能包含指定 key。
         * 通过 key 范围过滤，若 key 不在 [minKey, maxKey] 区间内则一定不包含。
         */
        public boolean mightContain(byte[] key) {
            if (minKey == null || maxKey == null) {
                return true; // 空文件，保守返回 true
            }
            return ByteUtil.compare(key, minKey) >= 0 && ByteUtil.compare(key, maxKey) <= 0;
        }
    }
}
