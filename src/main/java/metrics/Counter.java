package metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 线程安全的单调递增计数器。
 * 用于统计 QPS、操作次数等累计值。
 */
public class Counter {

    private final String name;
    private final String help;
    private final AtomicLong value = new AtomicLong(0);

    public Counter(String name, String help) {
        this.name = name;
        this.help = help;
    }

    public void increment() {
        value.incrementAndGet();
    }

    public void add(long delta) {
        value.addAndGet(delta);
    }

    public long get() {
        return value.get();
    }

    public void reset() {
        value.set(0);
    }

    public String getName() {
        return name;
    }

    public String getHelp() {
        return help;
    }
}
