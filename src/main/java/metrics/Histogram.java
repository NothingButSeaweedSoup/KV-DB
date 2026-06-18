package metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 直方图，用于统计延迟分布。
 * <p>
 * 使用 T-Digest 简化方案：维护 count、sum，以及预定义 bucket 的计数，
 * 可计算 P50/P95/P99 近似值。
 * <p>
 * 为控制内存，采用固定 bucket 边界（1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, +Inf）。
 */
public class Histogram {

    private static final long[] BUCKET_BOUNDARIES_MS = {
            1, 5, 10, 25, 50, 100, 250, 500, 1000
    };

    private final String name;
    private final String help;
    private final LongAdder count = new LongAdder();
    private final LongAdder sum = new LongAdder();
    private final LongAdder[] buckets;

    public Histogram(String name, String help) {
        this.name = name;
        this.help = help;
        this.buckets = new LongAdder[BUCKET_BOUNDARIES_MS.length + 1];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /**
     * 记录一次观测值（单位：毫秒）。
     */
    public void observe(long valueMs) {
        count.increment();
        sum.add(valueMs);
        for (int i = 0; i < BUCKET_BOUNDARIES_MS.length; i++) {
            if (valueMs <= BUCKET_BOUNDARIES_MS[i]) {
                buckets[i].increment();
                return;
            }
        }
        // 超出所有边界，落入 +Inf bucket
        buckets[buckets.length - 1].increment();
    }

    public long getCount() {
        return count.sum();
    }

    public long getSum() {
        return sum.sum();
    }

    /**
     * 获取 bucket 边界数组（毫秒），最后一个为 Long.MAX_VALUE 代表 +Inf。
     */
    public long[] getBucketBoundaries() {
        long[] boundaries = new long[BUCKET_BOUNDARIES_MS.length + 1];
        System.arraycopy(BUCKET_BOUNDARIES_MS, 0, boundaries, 0, BUCKET_BOUNDARIES_MS.length);
        boundaries[boundaries.length - 1] = Long.MAX_VALUE;
        return boundaries;
    }

    /**
     * 获取各 bucket 的累计计数。
     */
    public long[] getBucketCounts() {
        long[] counts = new long[buckets.length];
        for (int i = 0; i < buckets.length; i++) {
            counts[i] = buckets[i].sum();
        }
        return counts;
    }

    public String getName() {
        return name;
    }

    public String getHelp() {
        return help;
    }
}
