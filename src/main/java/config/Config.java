package config;

import core.FsyncStrategy;

public class Config {

    /**
     * 默认值常量，单一来源。Config.Builder 和 DBConfigLoader 均引用此常量。
     */
    public static final class Defaults {
        public static final long WAL_SEGMENT_SIZE = 1 * 1024 * 1024;       // 1MB
        public static final long MEM_TABLE_THRESHOLD = 4 * 1024 * 1024;    // 4MB
        public static final FsyncStrategy FSYNC_STRATEGY = FsyncStrategy.BATCH;
        public static final long SST_TARGET_FILE_SIZE = 8 * 1024 * 1024;   // 8MB
        public static final int LEVEL0_FILE_NUM_COMPACTION_TRIGGER = 4;

        private Defaults() {
        }
    }

    private final String dataDir;
    private final long walSegmentSize;
    private final long memTableThreshold;
    private final FsyncStrategy fsyncStrategy;
    private final long sstTargetFileSize;
    private final int level0FileNumCompactionTrigger;

    private Config(Builder builder) {
        this.dataDir = builder.dataDir;
        this.walSegmentSize = builder.walSegmentSize;
        this.memTableThreshold = builder.memTableThreshold;
        this.fsyncStrategy = builder.fsyncStrategy;
        this.sstTargetFileSize = builder.sstTargetFileSize;
        this.level0FileNumCompactionTrigger = builder.level0FileNumCompactionTrigger;
    }

    public String getDataDir() {
        return dataDir;
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

    public static class Builder {
        private String dataDir;
        private long walSegmentSize = Defaults.WAL_SEGMENT_SIZE;
        private long memTableThreshold = Defaults.MEM_TABLE_THRESHOLD;
        private FsyncStrategy fsyncStrategy = Defaults.FSYNC_STRATEGY;
        private long sstTargetFileSize = Defaults.SST_TARGET_FILE_SIZE;
        private int level0FileNumCompactionTrigger = Defaults.LEVEL0_FILE_NUM_COMPACTION_TRIGGER;

        public Builder setDataDir(String dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setWalSegmentSize(long walSegmentSize) {
            this.walSegmentSize = walSegmentSize;
            return this;
        }

        public Builder setMemTableThreshold(long memTableThreshold) {
            this.memTableThreshold = memTableThreshold;
            return this;
        }

        public Builder setFsyncStrategy(FsyncStrategy fsyncStrategy) {
            this.fsyncStrategy = fsyncStrategy;
            return this;
        }

        public Builder setSstTargetFileSize(long sstTargetFileSize) {
            this.sstTargetFileSize = sstTargetFileSize;
            return this;
        }

        public Builder setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) {
            this.level0FileNumCompactionTrigger = level0FileNumCompactionTrigger;
            return this;
        }

        public Config build() {
            if (dataDir == null || dataDir.trim().isEmpty()) {
                throw new IllegalArgumentException("dataDir不能为空");
            }
            if (walSegmentSize <= 0) {
                throw new IllegalArgumentException("walSegmentSize必须大于0");
            }
            if (memTableThreshold <= 0) {
                throw new IllegalArgumentException("memTable的阈值必须大于0");
            }
            if (fsyncStrategy == null) {
                throw new IllegalArgumentException("fsyncStrategy不能为null");
            }
            if (sstTargetFileSize <= 0) {
                throw new IllegalArgumentException("sstTargetFileSize必须大于0");
            }
            if (level0FileNumCompactionTrigger <= 0) {
                throw new IllegalArgumentException("level0FileNumCompactionTrigger必须大于0");
            }
            return new Config(this);
        }
    }
}
