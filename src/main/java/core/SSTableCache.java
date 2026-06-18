package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SSTable 缓存池，支持 LRU 淘汰与 mmap 读取。
 * <p>
 * 缓存已加载索引的 SSTable 对象，避免每次查询重新打开文件、加载索引和布隆过滤器。
 * 使用引用计数管理生命周期，配合 LRU 策略在缓存满时淘汰最久未访问的条目。
 * <p>
 * 支持两种读取模式：
 * <ul>
 *   <li>{@link ReadMode#FILE_CHANNEL} — 传统 FileChannel 读取（默认）</li>
 *   <li>{@link ReadMode#MMAP} — 内存映射文件读取，适合热数据频繁随机读</li>
 * </ul>
 */
public class SSTableCache {

    private static final Logger log = LoggerFactory.getLogger(SSTableCache.class);

    /** 默认最大缓存条目数 */
    private static final int DEFAULT_MAX_ENTRIES = 64;

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final int maxEntries;
    private final ReadMode readMode;
    private final AtomicLong accessCounter = new AtomicLong(0);

    public SSTableCache() {
        this(DEFAULT_MAX_ENTRIES, ReadMode.FILE_CHANNEL);
    }

    public SSTableCache(int maxEntries, ReadMode readMode) {
        this.maxEntries = maxEntries;
        this.readMode = readMode;
    }

    /**
     * 获取指定路径的 SSTable，若缓存中不存在则加载并缓存。
     * 调用方使用完毕后必须调用 {@link #release(String)} 释放引用。
     */
    public SSTable getSSTable(String filePath) throws IOException {
        // 缓存满时尝试淘汰
        if (cache.size() >= maxEntries) {
            evictLRU();
        }

        CacheEntry entry = cache.compute(filePath, (key, existing) -> {
            if (existing != null) {
                existing.refCount.incrementAndGet();
                existing.lastAccess = accessCounter.incrementAndGet();
                return existing;
            }
            try {
                SSTable sst = (readMode == ReadMode.MMAP)
                        ? new SSTable(key, true)   // mmap 模式
                        : new SSTable(key);         // FileChannel 模式
                return new CacheEntry(sst, 1, accessCounter.incrementAndGet());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return entry.sstable;
    }

    /**
     * 释放指定文件的引用。当引用计数归零时关闭并移除缓存。
     */
    public void release(String filePath) {
        cache.computeIfPresent(filePath, (key, entry) -> {
            entry.lastAccess = accessCounter.incrementAndGet();
            if (entry.refCount.decrementAndGet() <= 0) {
                try {
                    entry.sstable.close();
                } catch (IOException e) {
                    log.warn("关闭缓存的SSTable失败: {}", key, e);
                }
                return null;
            }
            return entry;
        });
    }

    /**
     * LRU 淘汰：移除引用计数为 0 且最久未访问的条目。
     * 如果所有条目都在使用中（refCount > 0），则跳过淘汰。
     */
    private void evictLRU() {
        String lruKey = null;
        long lruAccess = Long.MAX_VALUE;

        for (var entry : cache.entrySet()) {
            CacheEntry ce = entry.getValue();
            if (ce.refCount.get() <= 0 && ce.lastAccess < lruAccess) {
                lruAccess = ce.lastAccess;
                lruKey = entry.getKey();
            }
        }

        if (lruKey != null) {
            cache.computeIfPresent(lruKey, (key, entry) -> {
                if (entry.refCount.get() <= 0) {
                    try {
                        entry.sstable.close();
                    } catch (IOException e) {
                        log.warn("LRU淘汰关闭SSTable失败: {}", key, e);
                    }
                    log.debug("LRU淘汰SSTable: {}", key);
                    return null;
                }
                return entry; // 被其他线程引用了，跳过
            });
        }
    }

    /**
     * 关闭并清空所有缓存的 SSTable。
     */
    public void closeAll() {
        cache.forEach((key, entry) -> {
            try {
                entry.sstable.close();
            } catch (IOException e) {
                log.warn("关闭缓存的SSTable失败: {}", key, e);
            }
        });
        cache.clear();
    }

    /**
     * 获取当前缓存的 SSTable 数量。
     */
    public int size() {
        return cache.size();
    }

    /**
     * 获取最大缓存容量。
     */
    public int getMaxEntries() {
        return maxEntries;
    }

    /**
     * 获取当前读取模式。
     */
    public ReadMode getReadMode() {
        return readMode;
    }

    /**
     * 缓存读取模式。
     */
    public enum ReadMode {
        /** 传统 FileChannel 随机读取 */
        FILE_CHANNEL,
        /** 内存映射文件（mmap）读取，适合热数据 */
        MMAP
    }

    private static class CacheEntry {
        final SSTable sstable;
        final AtomicInteger refCount;
        volatile long lastAccess;

        CacheEntry(SSTable sstable, int refCount, long lastAccess) {
            this.sstable = sstable;
            this.refCount = new AtomicInteger(refCount);
            this.lastAccess = lastAccess;
        }
    }
}
