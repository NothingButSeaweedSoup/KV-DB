package core;

import util.ByteUtil;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {
    private final ConcurrentSkipListMap<byte[], byte[]> table;
    private final WALManager wal;
    private final AtomicLong size;
    private final long maxSize;

    public MemTable(WALManager wal, long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("内存表大小必须大于0");
        }
        this.table = new ConcurrentSkipListMap<>(ByteUtil::compare);
        this.wal = wal;
        this.size = new AtomicLong(0);
        this.maxSize = maxSize;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        if (value == null) {
            throw new IllegalArgumentException(Constants.Error.VALUE_NULL);
        }
        wal.log(Constants.Operation.PUT, key, value);
        putWithoutWAL(key, value);
    }

    public void putWithoutWAL(byte[] key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        long entrySize = key.length;
        if (value != null) {
            entrySize += value.length;
        }
        byte[] oldValue = table.put(key, value);
        if (oldValue != null) {
            size.addAndGet(entrySize - oldValue.length);
        } else {
            size.addAndGet(entrySize);
        }
    }

    public byte[] get(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        return table.get(key);
    }

    public void delete(byte[] key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        wal.log(Constants.Operation.DELETE, key, null);
        deleteWithoutWAL(key);
    }

    public void deleteWithoutWAL(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        byte[] oldValue = table.get(key);
        if (oldValue != null) {
            size.addAndGet(key.length - (key.length + oldValue.length));
        } else {
            size.addAndGet(key.length);
        }
        table.put(key, Constants.Tombstone.TOMBSTONE);
    }

    public long getSize() {
        return size.get();
    }

    public boolean isFull() {
        return size.get() > maxSize;
    }

    public void flush() throws IOException {
        table.clear();
        size.set(0);
    }

    public Iterable<ConcurrentSkipListMap.Entry<byte[], byte[]>> iterator() {
        return table.entrySet();
    }
}
