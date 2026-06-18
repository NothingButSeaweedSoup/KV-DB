package metrics;

import java.util.Map;
import java.util.StringJoiner;

/**
 * 将 MetricRegistry 中的指标导出为 Prometheus 文本格式。
 * <p>
 * 格式参考：https://prometheus.io/docs/instrumenting/exposition_formats/
 */
public class PrometheusExporter {

    private final MetricRegistry registry;

    public PrometheusExporter(MetricRegistry registry) {
        this.registry = registry;
    }

    /**
     * 导出所有指标为 Prometheus exposition format 文本。
     */
    public String export() {
        StringBuilder sb = new StringBuilder();

        // Counters
        for (Map.Entry<String, Counter> entry : registry.getCounters().entrySet()) {
            Counter c = entry.getValue();
            sb.append("# HELP ").append(c.getName()).append(" ").append(c.getHelp()).append("\n");
            sb.append("# TYPE ").append(c.getName()).append(" counter\n");
            sb.append(c.getName()).append(" ").append(c.get()).append("\n");
        }

        // Histograms
        for (Map.Entry<String, Histogram> entry : registry.getHistograms().entrySet()) {
            Histogram h = entry.getValue();
            String name = h.getName();
            sb.append("# HELP ").append(name).append(" ").append(h.getHelp()).append("\n");
            sb.append("# TYPE ").append(name).append(" histogram\n");

            long[] boundaries = h.getBucketBoundaries();
            long[] counts = h.getBucketCounts();
            long cumulative = 0;
            for (int i = 0; i < boundaries.length; i++) {
                cumulative += counts[i];
                String le = (boundaries[i] == Long.MAX_VALUE) ? "+Inf" : String.valueOf(boundaries[i]);
                sb.append(name).append("_bucket{le=\"").append(le).append("\"} ").append(cumulative).append("\n");
            }
            sb.append(name).append("_count ").append(h.getCount()).append("\n");
            sb.append(name).append("_sum ").append(h.getSum()).append("\n");
        }

        // Gauges
        for (Map.Entry<String, MetricRegistry.GaugeHolder> entry : registry.getGauges().entrySet()) {
            String name = entry.getKey();
            MetricRegistry.GaugeHolder holder = entry.getValue();
            sb.append("# HELP ").append(name).append(" ").append(holder.help()).append("\n");
            sb.append("# TYPE ").append(name).append(" gauge\n");
            sb.append(name).append(" ").append(holder.gauge().getValue()).append("\n");
        }

        return sb.toString();
    }
}
