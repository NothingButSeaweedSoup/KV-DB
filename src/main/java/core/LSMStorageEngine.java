package core;

import api.StorageEngine;
import config.Config;
import util.ByteUtil;
import util.DBConfigLoader;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class LSMStorageEngine implements StorageEngine {
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private MemTable memTable;
    private WALManager wal;
    private SSTable sstable;
    private Compaction compaction;
    private DBConfigLoader dbConfigLoader;

    public LSMStorageEngine(String path) throws IOException {
        dbConfigLoader = new DBConfigLoader();
        this.config = new Config.Builder()
                .setDataDir(path)
                .setWalSegmentSize(dbConfigLoader.getWalSegmentSize())
                .setMemTableThreshold(dbConfigLoader.getMemTableThreshold())
                .build();
        init();
    }

    private void init() throws IOException {
        wal = new WALManager(
                config.getDataDir(),
                config.getWalSegmentSize()
        );
        memTable = new MemTable(
                wal,
                config.getMemTableThreshold()
        );
        compaction = new Compaction(config);
        wal.init(new RecoveryHandler());
    }

    @Override
    public void put(byte[] key, Object value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        lock.writeLock().lock();
        try {
            memTable.put(key, serializeObject(value));
            if (memTable.isFull()) {
                flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Object get(byte[] key) throws IOException, ClassNotFoundException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        lock.readLock().lock();
        try {
            byte[] value = memTable.get(key);
            if (value == Constants.Tombstone.TOMBSTONE) {
                return deserializeObject(value);
            }
            value = findInSSTable(key);
            if (value != null && value != Constants.Tombstone.TOMBSTONE) {
                return deserializeObject(value);
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private byte[] findInSSTable(byte[] key) throws IOException {
        for (int level = 0; level < compaction.getMaxLevel(); level++) {
            // 获取当前等级的sstable文件列表
            File sstableDirFile = new File(config.getDataDir() + File.separator + "level-" + level);
            if (!sstableDirFile.exists()) {
                continue;
            }
            File[] sstableFiles = sstableDirFile.listFiles();
            if (sstableFiles == null) {
                continue;
            }
            Arrays.sort(sstableFiles, (o1, o2) ->
                    Long.compare(o2.lastModified(), o1.lastModified()));
            for (File sstableFile : sstableFiles) {
                SSTable sstable = new SSTable(sstableFile.getAbsolutePath());
                try {
                    byte[] value = sstable.get(key);
                    if (value != null) {
                        return value;
                    }
                } finally {
                    sstable.close();
                }
            }
        }
        return null;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        lock.writeLock().lock();
        try {
            memTable.delete(key);
            if (memTable.isFull()) {
                flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            Map<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
            for (Map.Entry<byte[], byte[]> entry : memTable.iterator()) {
                data.put(entry.getKey(), entry.getValue());
            }

            String sstablePath = config.getDataDir()
                    + File.separator + "level-0" + File.separator
                    + "sstable" + System.currentTimeMillis() + Constants.File.SST_EXTENSION;
            sstable = new SSTable(sstablePath, data);
            sstable.close();

            memTable.flush();

            compaction.checkAndCompact();

            wal.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private byte[] serializeObject(Object value) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

        objectOutputStream.writeObject(value);

        objectOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }

    private Object deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
         if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

        Object value = objectInputStream.readObject();

        objectInputStream.close();

        return value;
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (memTable.getSize() > 0) {
                flush();
            }
            if (wal != null) {
                wal.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private class RecoveryHandler implements WALManager.RecoveryCallback {
        @Override
        public void recoveryEntry(byte operation, byte[] key, byte[] value) throws IOException {
            if (operation == Constants.Operation.PUT) {
                memTable.putWithoutWAL(key, value);
            } else if (operation == Constants.Operation.DELETE) {
                memTable.deleteWithoutWAL(key);
            }
        }
    }
}
