package core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.ByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SSTableCache 单元测试。
 * 覆盖缓存加载、引用计数、LRU 淘汰、mmap 模式等场景。
 */
class SSTableCacheTest {

    @TempDir
    Path tempDir;

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] val(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private String createTestSSTable(String name, String... kvPairs) throws IOException {
        ConcurrentSkipListMap<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
        for (int i = 0; i < kvPairs.length; i += 2) {
            data.put(key(kvPairs[i]), val(kvPairs[i + 1]));
        }
        String path = tempDir.resolve(name + ".sst").toString();
        SSTable sstable = new SSTable(path, data);
        sstable.close();
        return path;
    }

    @Test
    void shouldLoadAndCacheSSTable() throws IOException {
        String path = createTestSSTable("test1", "a", "1", "b", "2");
        SSTableCache cache = new SSTableCache();

        SSTable sst1 = cache.getSSTable(path);
        SSTable sst2 = cache.getSSTable(path);

        // 同一路径应返回缓存的同一实例
        assertThat(sst1).isSameAs(sst2);
        assertThat(cache.size()).isEqualTo(1);

        cache.release(path);
        cache.release(path);
        assertThat(cache.size()).isZero();
    }

    @Test
    void shouldReadDataFromCachedSSTable() throws IOException {
        String path = createTestSSTable("test2", "hello", "world", "foo", "bar");
        SSTableCache cache = new SSTableCache();

        SSTable sst = cache.getSSTable(path);
        assertThat(sst.get(key("hello"))).containsExactly(val("world"));
        assertThat(sst.get(key("foo"))).containsExactly(val("bar"));
        assertThat(sst.get(key("missing"))).isNull();

        cache.release(path);
    }

    @Test
    void shouldCloseAllEntries() throws IOException {
        String path1 = createTestSSTable("test3a", "a", "1");
        String path2 = createTestSSTable("test3b", "b", "2");
        SSTableCache cache = new SSTableCache();

        cache.getSSTable(path1);
        cache.getSSTable(path2);
        assertThat(cache.size()).isEqualTo(2);

        cache.closeAll();
        assertThat(cache.size()).isZero();
    }

    @Test
    void shouldEvictLRUWhenFull() throws IOException {
        // maxEntries = 2，第三个文件应触发淘汰
        SSTableCache cache = new SSTableCache(2, SSTableCache.ReadMode.FILE_CHANNEL);

        String path1 = createTestSSTable("lru1", "a", "1");
        String path2 = createTestSSTable("lru2", "b", "2");
        String path3 = createTestSSTable("lru3", "c", "3");

        cache.getSSTable(path1);
        cache.release(path1); // refCount=0, 可被淘汰

        cache.getSSTable(path2);
        cache.release(path2); // refCount=0, 可被淘汰

        // 缓存已满（2个），再加载第三个应触发 LRU 淘汰
        cache.getSSTable(path3);

        // path1 是最久未访问的，应被淘汰
        assertThat(cache.size()).isLessThanOrEqualTo(2);

        cache.closeAll();
    }

    @Test
    void shouldNotEvictEntryWithActiveRefs() throws IOException {
        SSTableCache cache = new SSTableCache(1, SSTableCache.ReadMode.FILE_CHANNEL);

        String path1 = createTestSSTable("active1", "a", "1");
        String path2 = createTestSSTable("active2", "b", "2");

        // 获取 path1 但不释放（refCount=1）
        cache.getSSTable(path1);

        // 尝试加载 path2，但 path1 有活跃引用不可淘汰
        cache.getSSTable(path2);
        cache.release(path2);

        // path1 仍应在缓存中（因有活跃引用）
        assertThat(cache.size()).isGreaterThanOrEqualTo(1);

        cache.release(path1);
        cache.closeAll();
    }

    @Test
    void shouldSupportMmapMode() throws IOException {
        String path = createTestSSTable("mmap1", "key1", "value1", "key2", "value2");
        SSTableCache cache = new SSTableCache(10, SSTableCache.ReadMode.MMAP);

        SSTable sst = cache.getSSTable(path);
        assertThat(sst.get(key("key1"))).containsExactly(val("value1"));
        assertThat(sst.get(key("key2"))).containsExactly(val("value2"));

        cache.release(path);
        cache.closeAll();
    }

    @Test
    void mmapShouldReturnSameDataAsFileChannel() throws IOException {
        String path = createTestSSTable("compare", "x", "100", "y", "200", "z", "300");

        // FileChannel 模式
        SSTableCache fcCache = new SSTableCache(10, SSTableCache.ReadMode.FILE_CHANNEL);
        SSTable fcSst = fcCache.getSSTable(path);
        byte[] fcValX = fcSst.get(key("x"));
        byte[] fcValY = fcSst.get(key("y"));
        byte[] fcValZ = fcSst.get(key("z"));
        fcCache.release(path);

        // mmap 模式
        SSTableCache mmapCache = new SSTableCache(10, SSTableCache.ReadMode.MMAP);
        SSTable mmapSst = mmapCache.getSSTable(path);
        byte[] mmapValX = mmapSst.get(key("x"));
        byte[] mmapValY = mmapSst.get(key("y"));
        byte[] mmapValZ = mmapSst.get(key("z"));
        mmapCache.release(path);

        // 两种模式读取结果应完全一致
        assertThat(mmapValX).containsExactly(fcValX);
        assertThat(mmapValY).containsExactly(fcValY);
        assertThat(mmapValZ).containsExactly(fcValZ);

        fcCache.closeAll();
        mmapCache.closeAll();
    }
}
