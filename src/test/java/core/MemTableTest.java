package core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

class MemTableTest {

    private WALManager wal;
    private MemTable memTable;

    @BeforeEach
    void setUp() throws IOException {
        wal = mock(WALManager.class);
        doNothing().when(wal).log(any(Byte.class), any(byte[].class), any(byte[].class));
        memTable = new MemTable(wal, 1024);
    }

    @Test
    void shouldPutAndGet() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        memTable.put(key, value);
        assertThat(memTable.get(key)).containsExactly(value);
    }

    @Test
    void shouldUpdateExistingKey() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        memTable.put(key, value1);
        memTable.put(key, value2);
        assertThat(memTable.get(key)).containsExactly(value2);
    }

    @Test
    void shouldReturnNullForMissingKey() {
        assertThat(memTable.get("missing".getBytes())).isNull();
    }

    @Test
    void shouldDeleteKey() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        memTable.put(key, value);
        memTable.delete(key);
        assertThat(memTable.get(key)).containsExactly(Constants.Tombstone.TOMBSTONE);
    }

    @Test
    void shouldReportSizeChanges() throws IOException {
        byte[] key = "k".getBytes();
        byte[] value = "v".getBytes();
        long sizeBefore = memTable.getSize();
        memTable.put(key, value);
        assertThat(memTable.getSize()).isGreaterThan(sizeBefore);
    }

    @Test
    void shouldBecomeFullWhenThresholdReached() throws IOException {
        MemTable smallMemTable = new MemTable(wal, 10);
        byte[] key = "longkey".getBytes();
        byte[] value = "longvalue".getBytes();
        smallMemTable.put(key, value);
        assertThat(smallMemTable.isFull()).isTrue();
    }

    @Test
    void shouldRejectNullKeyOnPut() {
        assertThatThrownBy(() -> memTable.put(null, "value".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(Constants.Error.KEY_NULL);
    }

    @Test
    void shouldRejectNullValueOnPut() {
        assertThatThrownBy(() -> memTable.put("key".getBytes(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(Constants.Error.VALUE_NULL);
    }

    @Test
    void shouldRejectNullKeyOnGet() {
        assertThatThrownBy(() -> memTable.get(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(Constants.Error.KEY_NULL);
    }

    @Test
    void shouldRejectNullKeyOnDelete() {
        assertThatThrownBy(() -> memTable.delete(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(Constants.Error.KEY_NULL);
    }

    @Test
    void shouldFlushClearData() throws IOException {
        memTable.put("key".getBytes(), "value".getBytes());
        assertThat(memTable.getSize()).isPositive();
        memTable.flush();
        assertThat(memTable.getSize()).isZero();
        assertThat(memTable.get("key".getBytes())).isNull();
    }

    @Test
    void shouldSupportConcurrentPuts() throws InterruptedException, IOException {
        int threadCount = 10;
        int writesPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < writesPerThread; j++) {
                        byte[] key = ("thread-" + threadId + "-key-" + j).getBytes();
                        byte[] value = ("value-" + j).getBytes();
                        memTable.put(key, value);
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
                byte[] key = ("thread-" + i + "-key-" + j).getBytes();
                assertThat(memTable.get(key)).isNotNull();
            }
        }
    }
}
