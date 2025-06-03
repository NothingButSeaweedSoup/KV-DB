package core;

import config.Config;
import util.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Compaction {
    private final int maxLevel;
    private final Config config;
    private final String dbDir;
    private final long[] levelThreshold;

    public Compaction(Config config) {
        this.config = config;
        this.dbDir = config.getDataDir();
        this.maxLevel = 7;
        this.levelThreshold = new long[maxLevel + 1];
        this.levelThreshold[0] = 4;
        for (int i = 1; i < maxLevel; i++) {
            if (i == 1) {
                this.levelThreshold[i] = 10;
            } else {
                this.levelThreshold[i] = this.levelThreshold[i - 1] * 10;
            }
        }
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public void checkAndCompact() throws IOException {
        for (int i = 0; i < maxLevel; i++) {
            List<SSTable> ssTables = getSSTables(i);
            if (ssTables.size() >= levelThreshold[i] && i < maxLevel) {
                compact(i, ssTables);
            }
        }
    }

    private List<SSTable> getSSTables(int level) throws IOException {
        List<SSTable> ssTables = new ArrayList<>();
        Path levelPath = Path.of(dbDir, "level-" + level);

        if (!levelPath.toFile().exists()) {
            Files.createDirectories(levelPath);
            return ssTables;
        }
        Files.list(levelPath)
                .filter(path -> path.toString().endsWith(Constants.File.SST_EXTENSION))
                .sorted(Comparator.comparing(path -> {
                    try {
                        return Files.getLastModifiedTime(path);
                    } catch (IOException e) {
                        return null;
                    }
                }))
                .forEach(path -> {
                    try {
                        ssTables.add(new SSTable(path.toString()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        return ssTables;
    }

    private void compact(int level, List<SSTable> ssTables) throws IOException {
        Map<byte[], byte[]> tempMap = new ConcurrentSkipListMap<>(ByteUtil::compare);

        for (SSTable ssTable : ssTables) {
            tempMap.putAll(ssTable.getAll());
            ssTable.close();
        }

        Path newLevelPath = Path.of(dbDir, "level-" + (level + 1));
        if (!newLevelPath.toFile().exists()) {
            Files.createDirectories(newLevelPath);
        }

        String newSSTablePath = newLevelPath.toString()
                + Constants.System.FILE_SEPARATOR
                + "sstable"
                + System.currentTimeMillis()
                + Constants.File.SST_EXTENSION;
        SSTable newSSTable = new SSTable(newSSTablePath, tempMap);
        newSSTable.close();

        for (SSTable ssTable : ssTables) {
            new File(ssTable.getFilePath()).delete();
        }
    }

    public String getDbDir() {
        return dbDir;
    }
}
