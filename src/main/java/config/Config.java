package config;

import core.FsyncStrategy;

public class Config {
    private final String dataDir;
    private final long walSegmentSize;
    private final long memTableThreshold;
    private final FsyncStrategy fsyncStrategy;

    private Config(Builder builder) {
        this.dataDir = builder.dataDir;
        this.walSegmentSize = builder.walSegmentSize;
        this.memTableThreshold = builder.memTableThreshold;
        this.fsyncStrategy = builder.fsyncStrategy;
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

    public static class Builder {
        private String dataDir;
        private long walSegmentSize = 1 * 1024 * 1024;
        private long memTableThreshold = 4 * 1024 * 1024;
        private FsyncStrategy fsyncStrategy = FsyncStrategy.BATCH;

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
            return new Config(this);
        }
    }
}
