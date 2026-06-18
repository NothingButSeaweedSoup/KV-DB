package core;

import api.StorageEngine;
import config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serializer.JavaSerializer;
import serializer.Serializer;
import util.ByteUtil;
import util.DBConfigLoader;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LSMStorageEngine implements StorageEngine {

    private static final Logger log = LoggerFactory.getLogger(LSMStorageEngine.class);

    private final Config config;
    private final Serializer<Object> valueSerializer;
    private final WALManager wal;
    private final VersionSet versionSet;
    private final SSTableCache sstableCache;
    private final Compaction compaction;
    private final MemTableState memTableState;
    private final ExecutorService flushExecutor;

    public LSMStorageEngine(String path) throws IOException {
        this(path, new JavaSerializer<>());
    }

    public LSMStorageEngine(String path, Serializer<Object> valueSerializer) throws IOException {
        DBConfigLoader dbConfigLoader = new DBConfigLoader();
        this.config = new Config.Builder()
                .setDataDir(path)
                .setWalSegmentSize(dbConfigLoader.getWalSegmentSize())
                .setMemTableThreshold(dbConfigLoader.getMemTableThreshold())
                .setFsyncStrategy(dbConfigLoader.getFsyncStrategy())
                .setSstTargetFileSize(dbConfigLoader.getSstTargetFileSize())
                .setLevel0FileNumCompactionTrigger(dbConfigLoader.getLevel0FileNumCompactionTrigger())
                .build();
        this.valueSerializer = valueSerializer;
        this.wal = new WALManager(config.getDataDir(), config.getWalSegmentSize(), config.getFsyncStrategy());
        this.sstableCache = new SSTableCache();
        this.versionSet = new VersionSet(config.getDataDir(), 7);
        this.versionSet.loadFromDisk();
        this.compaction = new Compaction(config, versionSet);
        // memTableState 必须在 wal.init() 之前初始化，因为 RecoveryHandler 会引用它
        this.memTableState = new MemTableState(new MemTable(wal, config.getMemTableThreshold()));
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "memtable-flush");
            t.setDaemon(true);
            return t;
        });
        wal.init(new RecoveryHandler());
    }

    // 供子类（如 MasterNode）传入已有 Config 的构造器
    protected LSMStorageEngine(Config config) throws IOException {
        this.config = config;
        this.valueSerializer = new JavaSerializer<>();
        this.wal = new WALManager(config.getDataDir(), config.getWalSegmentSize(), config.getFsyncStrategy());
        this.sstableCache = new SSTableCache();
        this.versionSet = new VersionSet(config.getDataDir(), 7);
        this.versionSet.loadFromDisk();
        this.compaction = new Compaction(config, versionSet);
        // memTableState 必须在 wal.init() 之前初始化，因为 RecoveryHandler 会引用它
        this.memTableState = new MemTableState(new MemTable(wal, config.getMemTableThreshold()));
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "memtable-flush");
            t.setDaemon(true);
            return t;
        });
        wal.init(new RecoveryHandler());
    }

    @Override
    public void put(byte[] key, Object value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        memTableState.getActive().put(key, serializeObject(value));
        if (memTableState.isActiveFull()) {
            triggerFlush();
        }
    }

    @Override
    public Object get(byte[] key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        // 1. 查 MemTable（active + immutable）
        byte[] value = memTableState.get(key);
        if (value == Constants.Tombstone.TOMBSTONE) {
            return null;
        }
        if (value != null) {
            return deserializeObject(value);
        }
        // 2. 查 SSTable（通过 VersionSet 过滤）
        value = findInSSTable(key);
        if (value != null && value != Constants.Tombstone.TOMBSTONE) {
            return deserializeObject(value);
        }
        return null;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        memTableState.getActive().delete(key);
        if (memTableState.isActiveFull()) {
            triggerFlush();
        }
    }

    @Override
    public void flush() throws IOException {
        triggerFlush();
    }

    /**
     * 触发异步 flush：将 active 切换为 immutable，后台线程将 immutable 写入 Level 0 SSTable。
     */
    private void triggerFlush() {
        MemTable newActive = new MemTable(wal, config.getMemTableThreshold());
        if (!memTableState.switchActive(newActive)) {
            // 已有 immutable 正在 flush，跳过
            return;
        }
        MemTable immutable = memTableState.getImmutable();
        flushExecutor.submit(() -> {
            try {
                doFlush(immutable);
            } catch (IOException e) {
                log.error("MemTable flush失败", e);
            }
        });
    }

    /**
     * 将 immutable MemTable 的数据写入 Level 0 SSTable。
     */
    private void doFlush(MemTable immutable) throws IOException {
        Map<byte[], byte[]> data = immutable.snapshot();
        if (data.isEmpty()) {
            memTableState.clearImmutable();
            return;
        }

        String sstablePath = config.getDataDir()
                + File.separator + "level-0" + File.separator
                + "sstable" + System.currentTimeMillis()
                + Constants.File.SST_EXTENSION;

        SSTable sstable = new SSTable(sstablePath, data);
        byte[] minKey = sstable.getMinKey();
        byte[] maxKey = sstable.getMaxKey();
        int entries = sstable.getEntryCount();
        sstable.close();

        // 注册到 VersionSet
        versionSet.addSSTable(0, new VersionSet.SSTableMeta(sstablePath, minKey, maxKey, entries));

        // 清除 immutable
        memTableState.clearImmutable();

        // 异步触发 compaction
        compaction.checkAndCompactAsync();

        log.debug("MemTable flush完成: {} entries -> {}", entries, sstablePath);
    }

    private byte[] findInSSTable(byte[] key) throws IOException {
        java.util.List<VersionSet.SSTableMeta> candidates = versionSet.findCandidates(key);
        for (VersionSet.SSTableMeta meta : candidates) {
            SSTable sstable = sstableCache.getSSTable(meta.filePath());
            try {
                byte[] value = sstable.get(key);
                if (value != null) {
                    sstableCache.release(meta.filePath());
                    return value;
                }
            } finally {
                sstableCache.release(meta.filePath());
            }
        }
        return null;
    }

    private byte[] serializeObject(Object value) throws IOException {
        return valueSerializer.serialize(value);
    }

    private Object deserializeObject(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return valueSerializer.deserialize(bytes);
    }

    @Override
    public void close() throws IOException {
        // 1. 等待 flush 完成
        flushExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // 2. 关闭 compaction 线程池
        compaction.close();
        // 3. 如果 active 中还有数据，同步 flush 并注册到 VersionSet
        if (memTableState.getActive().getSize() > 0) {
            MemTable active = memTableState.getActive();
            Map<byte[], byte[]> data = active.snapshot();
            if (!data.isEmpty()) {
                String sstablePath = config.getDataDir()
                        + File.separator + "level-0" + File.separator
                        + "sstable" + System.currentTimeMillis()
                        + Constants.File.SST_EXTENSION;
                SSTable sstable = new SSTable(sstablePath, data);
                byte[] minKey = sstable.getMinKey();
                byte[] maxKey = sstable.getMaxKey();
                int entries = sstable.getEntryCount();
                sstable.close();
                versionSet.addSSTable(0, new VersionSet.SSTableMeta(sstablePath, minKey, maxKey, entries));
            }
        }
        // 4. 如果 immutable 中还有数据（flush 尚未完成），同步 flush
        MemTable immutable = memTableState.getImmutable();
        if (immutable != null && immutable.getSize() > 0) {
            Map<byte[], byte[]> data = immutable.snapshot();
            if (!data.isEmpty()) {
                String sstablePath = config.getDataDir()
                        + File.separator + "level-0" + File.separator
                        + "sstable" + System.currentTimeMillis()
                        + Constants.File.SST_EXTENSION;
                SSTable sstable = new SSTable(sstablePath, data);
                byte[] minKey = sstable.getMinKey();
                byte[] maxKey = sstable.getMaxKey();
                int entries = sstable.getEntryCount();
                sstable.close();
                versionSet.addSSTable(0, new VersionSet.SSTableMeta(sstablePath, minKey, maxKey, entries));
            }
            memTableState.clearImmutable();
        }
        // 4. 关闭缓存和 WAL
        sstableCache.closeAll();
        if (wal != null) {
            wal.close();
        }
    }

    private class RecoveryHandler implements WALManager.RecoveryCallback {
        @Override
        public void recoveryEntry(byte operation, byte[] key, byte[] value) throws IOException {
            if (operation == Constants.Operation.PUT) {
                memTableState.getActive().putWithoutWAL(key, value);
            } else if (operation == Constants.Operation.DELETE) {
                memTableState.getActive().deleteWithoutWAL(key);
            }
        }
    }
}
