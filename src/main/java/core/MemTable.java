package core;

import util.ByteUtil;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {

    /**
     * 每条记录的固定开销（跳表节点指针、对象头等）
     */
    private static final int ENTRY_OVERHEAD = 32;
    /**
     * 墓碑标记的固定开销
     */
    private static final int TOMBSTONE_OVERHEAD = 16;

    private final ConcurrentSkipListMap<byte[], byte[]> table;
    private final WALManager wal;
    private final AtomicLong size;
    private final long maxSize;
    private volatile boolean immutable;

    public MemTable(WALManager wal, long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("内存表大小必须大于0");
        }
        this.table = new ConcurrentSkipListMap<>(ByteUtil::compare);
        this.wal = wal;
        this.size = new AtomicLong(0);
        this.maxSize = maxSize;
        this.immutable = false;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        if (immutable) {
            throw new IllegalStateException("不可变MemTable不支持写入");
        }
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
        if (immutable) {
            throw new IllegalStateException("不可变MemTable不支持写入");
        }
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        long newEntrySize = key.length + (value != null ? value.length : 0) + ENTRY_OVERHEAD;
        byte[] oldValue = table.put(key, value);
        if (oldValue != null) {
            // 更新已有key：新值大小 - 旧值大小（旧值不含key的开销，因为key未变）
            long oldEntrySize = oldValue.length + ENTRY_OVERHEAD;
            size.addAndGet(newEntrySize - oldEntrySize);
        } else {
            // 新增key
            size.addAndGet(newEntrySize);
        }
    }

    public byte[] get(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        return table.get(key);
    }

    public void delete(byte[] key) throws IOException {
        if (immutable) {
            throw new IllegalStateException("不可变MemTable不支持写入");
        }
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        wal.log(Constants.Operation.DELETE, key, null);
        deleteWithoutWAL(key);
    }

    public void deleteWithoutWAL(byte[] key) {
        if (immutable) {
            throw new IllegalStateException("不可变MemTable不支持写入");
        }
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        byte[] oldValue = table.get(key);
        if (oldValue != null) {
            // 已有key：移除旧value开销，替换为墓碑开销
            long oldEntrySize = oldValue.length + ENTRY_OVERHEAD;
            size.addAndGet(TOMBSTONE_OVERHEAD - oldEntrySize);
        } else {
            // 新key写入墓碑：需要存储key + 墓碑开销
            size.addAndGet(key.length + TOMBSTONE_OVERHEAD);
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

    /**
     * 将此MemTable冻结为不可变状态，冻结后不支持写入操作。
     */
    public void freeze() {
        this.immutable = true;
    }

    /**
     * 撤销冻结，将此MemTable恢复为可变状态。
     * 仅用于 switchActive CAS 失败时的回滚。
     */
    public void unfreeze() {
        this.immutable = false;
    }

    /**
     * 返回此MemTable是否为不可变状态。
     */
    public boolean isImmutable() {
        return immutable;
    }

    /**
     * 返回数据的不可变快照视图。
     */
    public Map<byte[], byte[]> snapshot() {
        return Map.copyOf(table);
    }

    public Iterable<ConcurrentSkipListMap.Entry<byte[], byte[]>> iterator() {
        return table.entrySet();
    }
}
