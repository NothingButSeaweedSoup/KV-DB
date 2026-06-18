package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.Config;
import core.FsyncStrategy;

import java.io.File;
import java.io.IOException;

public class DBConfigLoader {
    private long walSegmentSize;
    private long memTableThreshold;
    private FsyncStrategy fsyncStrategy;
    private long sstTargetFileSize;
    private int level0FileNumCompactionTrigger;
    private static final String DB_CONFIG_FILE_PATH = "db-config.json";

    // 从配置文件中加载数据库配置
    public DBConfigLoader() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        // 判断配置文件是否存在
        if (new File(DB_CONFIG_FILE_PATH).exists()) {
            var jsonNode = objectMapper.readTree(new File(DB_CONFIG_FILE_PATH));
            this.walSegmentSize = jsonNode.has("walSegmentSize") ? jsonNode.get("walSegmentSize").asLong() : Config.Defaults.WAL_SEGMENT_SIZE;
            this.memTableThreshold = jsonNode.has("memTableThreshold") ? jsonNode.get("memTableThreshold").asLong() : Config.Defaults.MEM_TABLE_THRESHOLD;
            this.sstTargetFileSize = jsonNode.has("sstTargetFileSize") ? jsonNode.get("sstTargetFileSize").asLong() : Config.Defaults.SST_TARGET_FILE_SIZE;
            this.level0FileNumCompactionTrigger = jsonNode.has("level0FileNumCompactionTrigger") ? jsonNode.get("level0FileNumCompactionTrigger").asInt() : Config.Defaults.LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
            if (jsonNode.has("fsyncStrategy")) {
                String strategyName = jsonNode.get("fsyncStrategy").asText().toUpperCase();
                try {
                    this.fsyncStrategy = FsyncStrategy.valueOf(strategyName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "无效的fsyncStrategy配置值: " + strategyName +
                            "，有效值为: SYNC, BATCH, ASYNC", e);
                }
            } else {
                this.fsyncStrategy = FsyncStrategy.BATCH;
            }
        } else {
            this.walSegmentSize = Config.Defaults.WAL_SEGMENT_SIZE;
            this.memTableThreshold = Config.Defaults.MEM_TABLE_THRESHOLD;
            this.fsyncStrategy = Config.Defaults.FSYNC_STRATEGY;
            this.sstTargetFileSize = Config.Defaults.SST_TARGET_FILE_SIZE;
            this.level0FileNumCompactionTrigger = Config.Defaults.LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
        }
    }

    public long getWalSegmentSize() {
        return walSegmentSize;
    }

    public long getMemTableThreshold() {
        return memTableThreshold;
    }

    public FsyncStrategy getFsyncStrategy() {
        return fsyncStrategy;
    }

    public long getSstTargetFileSize() {
        return sstTargetFileSize;
    }

    public int getLevel0FileNumCompactionTrigger() {
        return level0FileNumCompactionTrigger;
    }
}
