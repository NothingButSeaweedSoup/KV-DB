package core;

import config.Config;
import metrics.MetricRegistry;
import metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 异步 Leveled Compaction 管理器。
 * <p>
 * 将 Compaction 拆分为独立后台线程，与前台写路径解耦。
 * 合并结果按 key 范围拆分为固定大小的新 SSTable，并通过 VersionSet 原子替换。
 */
public class Compaction {

    private static final Logger log = LoggerFactory.getLogger(Compaction.class);

    private final Config config;
    private final String dbDir;
    private final int maxLevel;
    private final long[] levelThreshold;
    private final long targetFileSize;
    private final VersionSet versionSet;
    private final ExecutorService compactionExecutor;
    private volatile boolean running = true;

    public Compaction(Config config, VersionSet versionSet) {
        this.config = config;
        this.dbDir = config.getDataDir();
        this.maxLevel = 7;
        this.targetFileSize = config.getSstTargetFileSize();
        this.versionSet = versionSet;
        this.levelThreshold = new long[maxLevel + 1];
        this.levelThreshold[0] = config.getLevel0FileNumCompactionTrigger();
        for (int i = 1; i < maxLevel; i++) {
            this.levelThreshold[i] = (i == 1) ? 10 : this.levelThreshold[i - 1] * 10;
        }
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "compaction-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    /**
     * 异步检查并触发 compaction。不阻塞调用线程。
     */
    public void checkAndCompactAsync() {
        if (!running) {
            return;
        }
        compactionExecutor.submit(() -> {
            try {
                checkAndCompact();
            } catch (IOException e) {
                log.error("Compaction执行失败", e);
            }
        });
    }

    /**
     * 同步检查并触发 compaction（用于启动时恢复）。
     */
    public void checkAndCompact() throws IOException {
        for (int i = 0; i < maxLevel; i++) {
            int levelSize = versionSet.getLevelSize(i);
            if (levelSize >= levelThreshold[i] && i < maxLevel) {
                doCompact(i);
            }
        }
    }

    private void doCompact(int level) throws IOException {
        long start = System.currentTimeMillis();
        List<VersionSet.SSTableMeta> levelMetas = versionSet.getLevel(level);
        if (levelMetas.isEmpty()) {
            return;
        }

        // 读取当前层所有 SSTable 数据
        Map<byte[], byte[]> mergedData = new ConcurrentSkipListMap<>(ByteUtil::compare);
        List<VersionSet.SSTableMeta> removedMetas = new ArrayList<>();

        for (VersionSet.SSTableMeta meta : levelMetas) {
            try {
                SSTable ssTable = new SSTable(meta.filePath());
                mergedData.putAll(ssTable.getAll());
                ssTable.close();
                removedMetas.add(meta);
            } catch (IOException e) {
                log.error("读取SSTable失败: {}", meta.filePath(), e);
            }
        }

        if (mergedData.isEmpty()) {
            return;
        }

        // 与下一层合并（若下一层有重叠的 key 范围）
        int nextLevel = level + 1;
        if (nextLevel < maxLevel) {
            List<VersionSet.SSTableMeta> nextLevelMetas = versionSet.getLevel(nextLevel);
            List<VersionSet.SSTableMeta> overlapping = findOverlapping(mergedData, nextLevelMetas);
            for (VersionSet.SSTableMeta meta : overlapping) {
                try {
                    SSTable ssTable = new SSTable(meta.filePath());
                    mergedData.putAll(ssTable.getAll());
                    ssTable.close();
                    removedMetas.add(meta);
                } catch (IOException e) {
                    log.error("读取SSTable失败: {}", meta.filePath(), e);
                }
            }
            // 从下一层移除重叠的元数据
            if (!overlapping.isEmpty()) {
                versionSet.replaceSSTables(nextLevel, overlapping, List.of());
            }
        }

        // 按 key 范围拆分为多个 SSTable
        Path newLevelPath = Path.of(dbDir, "level-" + nextLevel);
        if (!newLevelPath.toFile().exists()) {
            Files.createDirectories(newLevelPath);
        }

        List<VersionSet.SSTableMeta> newMetas = splitAndWrite(mergedData, newLevelPath);

        // 原子更新 VersionSet
        versionSet.replaceSSTables(level, removedMetas, List.of());
        if (nextLevel < maxLevel) {
            for (VersionSet.SSTableMeta meta : newMetas) {
                versionSet.addSSTable(nextLevel, meta);
            }
        }

        // 异步删除旧文件
        for (VersionSet.SSTableMeta meta : removedMetas) {
            new File(meta.filePath()).delete();
        }

        MetricRegistry reg = MetricRegistry.getInstance();
        reg.counter(Metrics.COMPACTION_COUNT, "").increment();
        reg.histogram(Metrics.COMPACTION_DURATION, "").observe(System.currentTimeMillis() - start);
        log.info("Compaction完成: level-{} -> level-{}, 输入{}个文件, 输出{}个文件",
                level, nextLevel, removedMetas.size(), newMetas.size());
    }

    /**
     * 查找与给定数据 key 范围重叠的 SSTable 元数据。
     * 不假设 Map 具体类型，通过遍历获取最小/最大 key。
     */
    private List<VersionSet.SSTableMeta> findOverlapping(
            Map<byte[], byte[]> data, List<VersionSet.SSTableMeta> metas) {
        if (data.isEmpty() || metas.isEmpty()) {
            return List.of();
        }

        byte[] rangeMin = null;
        byte[] rangeMax = null;
        for (byte[] key : data.keySet()) {
            if (rangeMin == null || ByteUtil.compare(key, rangeMin) < 0) {
                rangeMin = key;
            }
            if (rangeMax == null || ByteUtil.compare(key, rangeMax) > 0) {
                rangeMax = key;
            }
        }

        List<VersionSet.SSTableMeta> overlapping = new ArrayList<>();
        for (VersionSet.SSTableMeta meta : metas) {
            if (meta.minKey() == null || meta.maxKey() == null) {
                overlapping.add(meta);
                continue;
            }
            // 判断范围是否重叠：meta.maxKey >= rangeMin && meta.minKey <= rangeMax
            if (ByteUtil.compare(meta.maxKey(), rangeMin) >= 0
                    && ByteUtil.compare(meta.minKey(), rangeMax) <= 0) {
                overlapping.add(meta);
            }
        }
        return overlapping;
    }

    /**
     * 将合并后的数据按 targetFileSize 拆分为多个 SSTable 并写入磁盘。
     */
    private List<VersionSet.SSTableMeta> splitAndWrite(
            Map<byte[], byte[]> data, Path targetDir) throws IOException {
        List<VersionSet.SSTableMeta> result = new ArrayList<>();
        Map<byte[], byte[]> currentBatch = new ConcurrentSkipListMap<>(ByteUtil::compare);
        long currentBatchSize = 0;

        for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
            long entrySize = entry.getKey().length + (entry.getValue() != null ? entry.getValue().length : 0) + 8;
            if (!currentBatch.isEmpty() && currentBatchSize + entrySize > targetFileSize) {
                // 当前批次已满，写入 SSTable
                result.add(writeSSTable(currentBatch, targetDir));
                currentBatch = new ConcurrentSkipListMap<>(ByteUtil::compare);
                currentBatchSize = 0;
            }
            currentBatch.put(entry.getKey(), entry.getValue());
            currentBatchSize += entrySize;
        }

        // 写入最后一批
        if (!currentBatch.isEmpty()) {
            result.add(writeSSTable(currentBatch, targetDir));
        }

        return result;
    }

    private VersionSet.SSTableMeta writeSSTable(Map<byte[], byte[]> data, Path targetDir) throws IOException {
        String path = targetDir.toString()
                + Constants.System.FILE_SEPARATOR
                + "sstable" + System.currentTimeMillis()
                + Constants.File.SST_EXTENSION;
        SSTable sstable = new SSTable(path, data);
        byte[] minKey = sstable.getMinKey();
        byte[] maxKey = sstable.getMaxKey();
        int entries = sstable.getEntryCount();
        sstable.close();
        return new VersionSet.SSTableMeta(path, minKey, maxKey, entries);
    }

    /**
     * 关闭 compaction 线程池，等待正在执行的任务完成。
     */
    public void close() {
        running = false;
        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
