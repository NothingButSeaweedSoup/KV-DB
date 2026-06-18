package util;

import com.fasterxml.jackson.databind.ObjectMapper;
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
            this.walSegmentSize = jsonNode.has("walSegmentSize") ? jsonNode.get("walSegmentSize").asLong() : 1024 * 1024;
            this.memTableThreshold = jsonNode.has("memTableThreshold") ? jsonNode.get("memTableThreshold").asLong() : 4 * 1024 * 1024;
            this.sstTargetFileSize = jsonNode.has("sstTargetFileSize") ? jsonNode.get("sstTargetFileSize").asLong() : 8 * 1024 * 1024;
            this.level0FileNumCompactionTrigger = jsonNode.has("level0FileNumCompactionTrigger") ? jsonNode.get("level0FileNumCompactionTrigger").asInt() : 4;
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
            this.walSegmentSize = 1024 * 1024;
            this.memTableThreshold = 4 * 1024 * 1024;
            this.fsyncStrategy = FsyncStrategy.BATCH;
            this.sstTargetFileSize = 8 * 1024 * 1024;
            this.level0FileNumCompactionTrigger = 4;
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
