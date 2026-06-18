package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SSTable 缓存池。
 * <p>
 * 缓存已加载索引的 SSTable 对象，避免每次查询重新打开文件、加载索引和布隆过滤器。
 * 使用引用计数管理生命周期。
 */
public class SSTableCache {

    private static final Logger log = LoggerFactory.getLogger(SSTableCache.class);

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    /**
     * 获取指定路径的 SSTable，若缓存中不存在则加载并缓存。
     * 调用方使用完毕后必须调用 {@link #release(String)} 释放引用。
     */
    public SSTable getSSTable(String filePath) throws IOException {
        CacheEntry entry = cache.compute(filePath, (key, existing) -> {
            if (existing != null) {
                existing.refCount.incrementAndGet();
                return existing;
            }
            try {
                SSTable sst = new SSTable(key);
                return new CacheEntry(sst, 1);
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

    private static class CacheEntry {
        final SSTable sstable;
        final AtomicInteger refCount;

        CacheEntry(SSTable sstable, int refCount) {
            this.sstable = sstable;
            this.refCount = new AtomicInteger(refCount);
        }
    }
}
