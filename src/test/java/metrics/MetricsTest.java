package metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsTest {

    @BeforeEach
    void setUp() {
        MetricRegistry.getInstance().reset();
    }

    @AfterEach
    void tearDown() {
        MetricRegistry.getInstance().reset();
    }

    @Test
    void counterShouldIncrement() {
        Counter counter = MetricRegistry.getInstance().counter("test_counter", "test");
        counter.increment();
        counter.increment();
        counter.add(5);
        assertThat(counter.get()).isEqualTo(7);
    }

    @Test
    void counterShouldReset() {
        Counter counter = MetricRegistry.getInstance().counter("test_counter", "test");
        counter.add(100);
        counter.reset();
        assertThat(counter.get()).isZero();
    }

    @Test
    void histogramShouldTrackLatency() {
        Histogram histogram = MetricRegistry.getInstance().histogram("test_latency", "test");
        histogram.observe(1);
        histogram.observe(5);
        histogram.observe(10);
        histogram.observe(100);
        histogram.observe(1000);

        assertThat(histogram.getCount()).isEqualTo(5);
        assertThat(histogram.getSum()).isEqualTo(1116);

        long[] counts = histogram.getBucketCounts();
        // 1ms -> bucket[0], 5ms -> bucket[1], 10ms -> bucket[2], 100ms -> bucket[5], 1000ms -> bucket[8]
        assertThat(counts[0]).isEqualTo(1); // le=1
        assertThat(counts[1]).isEqualTo(1); // le=5
        assertThat(counts[2]).isEqualTo(1); // le=10
        assertThat(counts[5]).isEqualTo(1); // le=100
        assertThat(counts[8]).isEqualTo(1); // le=1000
    }

    @Test
    void histogramShouldTrackBuckets() {
        Histogram histogram = MetricRegistry.getInstance().histogram("test_bucket", "test");
        // All values within 5ms
        for (int i = 0; i < 100; i++) {
            histogram.observe(i % 6); // 0,1,2,3,4,5,0,1,2,3...
        }
        assertThat(histogram.getCount()).isEqualTo(100);
        long[] counts = histogram.getBucketCounts();
        // 0ms and 1ms go to bucket[0] (le=1), 2-5ms go to bucket[1] (le=5)
        assertThat(counts[0] + counts[1]).isEqualTo(100);
    }

    @Test
    void gaugeShouldTrackValue() {
        final double[] val = {42.0};
        MetricRegistry.getInstance().gauge("test_gauge", "test", () -> val[0]);

        MetricRegistry.GaugeHolder holder = MetricRegistry.getInstance().getGauges().get("test_gauge");
        assertThat(holder).isNotNull();
        assertThat(holder.gauge().getValue()).isEqualTo(42.0);

        val[0] = 100.0;
        assertThat(holder.gauge().getValue()).isEqualTo(100.0);
    }

    @Test
    void prometheusExporterShouldFormatCounters() {
        MetricRegistry reg = MetricRegistry.getInstance();
        reg.counter("test_total", "A test counter").add(42);

        PrometheusExporter exporter = new PrometheusExporter(reg);
        String output = exporter.export();

        assertThat(output).contains("# HELP test_total A test counter");
        assertThat(output).contains("# TYPE test_total counter");
        assertThat(output).contains("test_total 42");
    }

    @Test
    void prometheusExporterShouldFormatHistograms() {
        MetricRegistry reg = MetricRegistry.getInstance();
        Histogram h = reg.histogram("test_latency_ms", "Test latency");
        h.observe(3);
        h.observe(50);

        PrometheusExporter exporter = new PrometheusExporter(reg);
        String output = exporter.export();

        assertThat(output).contains("# TYPE test_latency_ms histogram");
        assertThat(output).contains("test_latency_ms_bucket{le=\"5\"}");
        assertThat(output).contains("test_latency_ms_bucket{le=\"+Inf\"}");
        assertThat(output).contains("test_latency_ms_count 2");
        assertThat(output).contains("test_latency_ms_sum 53");
    }

    @Test
    void prometheusExporterShouldFormatGauges() {
        MetricRegistry reg = MetricRegistry.getInstance();
        reg.gauge("test_bytes", "Test bytes", () -> 1024.0);

        PrometheusExporter exporter = new PrometheusExporter(reg);
        String output = exporter.export();

        assertThat(output).contains("# TYPE test_bytes gauge");
        assertThat(output).contains("test_bytes 1024.0");
    }

    @Test
    void metricsHttpServerShouldStartAndStop() throws Exception {
        MetricRegistry reg = MetricRegistry.getInstance();
        reg.counter("test_metric", "test").add(1);

        // Use a random high port to avoid conflicts
        MetricsHttpServer server = new MetricsHttpServer(18932, reg);
        server.start();
        // Server started successfully - verify no exception was thrown
        server.stop();
    }
}
