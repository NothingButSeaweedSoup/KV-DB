package util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class BloomFilterTest {

    @Test
    void shouldMatchAddedElements() {
        BloomFilter filter = new BloomFilter(1000, 0.01);
        byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
        filter.add(key);
        assertThat(filter.mightMatch(key)).isTrue();
    }

    @Test
    void shouldKeepFalsePositiveRateLow() {
        int expected = 10000;
        double fpp = 0.01;
        BloomFilter filter = new BloomFilter(expected, fpp);

        Set<String> inserted = new HashSet<>();
        for (int i = 0; i < expected; i++) {
            String key = "key-" + i;
            filter.add(key.getBytes(StandardCharsets.UTF_8));
            inserted.add(key);
        }

        int falsePositives = 0;
        int checkCount = 10000;
        for (int i = expected; i < expected + checkCount; i++) {
            String key = "key-" + i;
            if (filter.mightMatch(key.getBytes(StandardCharsets.UTF_8))) {
                falsePositives++;
            }
        }

        double actualFpp = (double) falsePositives / checkCount;
        assertThat(actualFpp).isLessThan(fpp * 2);
    }

    @Test
    void shouldClearAllBits() {
        BloomFilter filter = new BloomFilter(100, 0.01);
        byte[] key = "test".getBytes(StandardCharsets.UTF_8);
        filter.add(key);
        assertThat(filter.mightMatch(key)).isTrue();

        filter.clear();
        assertThat(filter.mightMatch(key)).isFalse();
    }

    @Test
    void shouldReportSizeAndHashCount() {
        BloomFilter filter = new BloomFilter(1000, 0.01);
        assertThat(filter.getSize()).isPositive();
        assertThat(filter.getHashCount()).isPositive();
    }
}
