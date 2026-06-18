package core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

class MemTableStateTest {

    private WALManager wal;

    @BeforeEach
    void setUp() throws IOException {
        wal = mock(WALManager.class);
        doNothing().when(wal).log(any(Byte.class), any(byte[].class), any(byte[].class));
    }

    @Test
    void shouldGetFromActive() throws IOException {
        MemTable active = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active);
        active.put("key".getBytes(), "value".getBytes());
        assertThat(state.get("key".getBytes())).containsExactly("value".getBytes());
    }

    @Test
    void shouldGetFromImmutableAfterSwitch() throws IOException {
        MemTable active1 = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active1);
        active1.put("old-key".getBytes(), "old-value".getBytes());

        MemTable active2 = new MemTable(wal, 1024);
        assertThat(state.switchActive(active2)).isTrue();

        // immutable 中的数据仍可查到
        assertThat(state.get("old-key".getBytes())).containsExactly("old-value".getBytes());
        // active 中的数据也可查到
        active2.put("new-key".getBytes(), "new-value".getBytes());
        assertThat(state.get("new-key".getBytes())).containsExactly("new-value".getBytes());
    }

    @Test
    void activeOverridesImmutable() throws IOException {
        MemTable active1 = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active1);
        active1.put("key".getBytes(), "old".getBytes());

        MemTable active2 = new MemTable(wal, 1024);
        state.switchActive(active2);
        active2.put("key".getBytes(), "new".getBytes());

        // active 的值覆盖 immutable
        assertThat(state.get("key".getBytes())).containsExactly("new".getBytes());
    }

    @Test
    void switchActiveFailsWhenImmutableExists() throws IOException {
        MemTable active1 = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active1);

        MemTable active2 = new MemTable(wal, 1024);
        assertThat(state.switchActive(active2)).isTrue();
        assertThat(state.hasImmutable()).isTrue();

        // 第二次切换应失败
        MemTable active3 = new MemTable(wal, 1024);
        assertThat(state.switchActive(active3)).isFalse();
    }

    @Test
    void clearImmutableAllowsNewSwitch() throws IOException {
        MemTable active1 = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active1);

        MemTable active2 = new MemTable(wal, 1024);
        state.switchActive(active2);
        assertThat(state.hasImmutable()).isTrue();

        state.clearImmutable();
        assertThat(state.hasImmutable()).isFalse();

        // 现在可以再次切换
        MemTable active3 = new MemTable(wal, 1024);
        assertThat(state.switchActive(active3)).isTrue();
    }

    @Test
    void versionIncrementsOnSwitch() throws IOException {
        MemTable active1 = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active1);
        assertThat(state.getVersion()).isEqualTo(0);

        state.switchActive(new MemTable(wal, 1024));
        assertThat(state.getVersion()).isEqualTo(1);

        state.clearImmutable();
        state.switchActive(new MemTable(wal, 1024));
        assertThat(state.getVersion()).isEqualTo(2);
    }

    @Test
    void immutableRejectsWrites() throws IOException {
        MemTable active = new MemTable(wal, 1024);
        MemTableState state = new MemTableState(active);
        state.switchActive(new MemTable(wal, 1024));

        MemTable immutable = state.getImmutable();
        assertThat(immutable).isNotNull();
        assertThatThrownBy(() -> immutable.put("k".getBytes(), "v".getBytes()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void concurrentReadsAndWrites() throws InterruptedException, IOException {
        MemTable active = new MemTable(wal, 100000);
        MemTableState state = new MemTableState(active);
        int threadCount = 8;
        int writesPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < writesPerThread; j++) {
                        byte[] key = ("t" + id + "-k" + j).getBytes();
                        byte[] val = ("v" + j).getBytes();
                        state.getActive().put(key, val);
                        assertThat(state.get(key)).isNotNull();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < writesPerThread; j++) {
                assertThat(state.get(("t" + i + "-k" + j).getBytes())).isNotNull();
            }
        }
    }
}
