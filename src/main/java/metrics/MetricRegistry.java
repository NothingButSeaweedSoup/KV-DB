package metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 指标注册中心，全局单例。
 * <p>
 * 所有 Counter、Histogram、Gauge 通过此类注册与获取，PrometheusExporter 从这里读取全部指标。
 */
public class MetricRegistry {

    private static final Logger log = LoggerFactory.getLogger(MetricRegistry.class);
    private static final MetricRegistry INSTANCE = new MetricRegistry();

    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, GaugeHolder> gauges = new ConcurrentHashMap<>();

    private MetricRegistry() {
    }

    public static MetricRegistry getInstance() {
        return INSTANCE;
    }

    // ---- Counter ----

    public Counter counter(String name, String help) {
        return counters.computeIfAbsent(name, n -> new Counter(n, help));
    }

    // ---- Histogram ----

    public Histogram histogram(String name, String help) {
        return histograms.computeIfAbsent(name, n -> new Histogram(n, help));
    }

    // ---- Gauge ----

    public void gauge(String name, String help, Gauge gauge) {
        gauges.put(name, new GaugeHolder(help, gauge));
    }

    // ---- 读取全部指标（供 Exporter 使用）----

    public Map<String, Counter> getCounters() {
        return Collections.unmodifiableMap(counters);
    }

    public Map<String, Histogram> getHistograms() {
        return Collections.unmodifiableMap(histograms);
    }

    public Map<String, GaugeHolder> getGauges() {
        return Collections.unmodifiableMap(gauges);
    }

    /**
     * 重置所有计数器（测试用）。
     */
    public void reset() {
        counters.clear();
        histograms.clear();
        gauges.clear();
    }

    public record GaugeHolder(String help, Gauge gauge) {
    }
}
