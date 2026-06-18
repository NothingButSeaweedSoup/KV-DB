package metrics;

import core.Compaction;
import core.LSMStorageEngine;
import core.MemTableState;
import core.WALManager;
import core.VersionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV-DB 指标定义与注册入口。
 * <p>
 * 所有指标名称、帮助文本集中定义，各模块通过此类注册和记录指标。
 * 提供 {@link #init(MetricsConfig)} 一次性完成注册与 Gauge 绑定。
 */
public final class Metrics {

    private Metrics() {
    }

    // ---- 指标名称常量 ----

    // Counters
    public static final String PUT_COUNT = "kvdb_put_total";
    public static final String GET_COUNT = "kvdb_get_total";
    public static final String DELETE_COUNT = "kvdb_delete_total";
    public static final String COMPACTION_COUNT = "kvdb_compaction_total";
    public static final String FLUSH_COUNT = "kvdb_flush_total";
    public static final String WAL_ROTATE_COUNT = "kvdb_wal_rotate_total";
    public static final String WAL_CRC_FAILURE_COUNT = "kvdb_wal_crc_failure_total";

    // Histograms (毫秒)
    public static final String PUT_LATENCY = "kvdb_put_latency_ms";
    public static final String GET_LATENCY = "kvdb_get_latency_ms";
    public static final String COMPACTION_DURATION = "kvdb_compaction_duration_ms";
    public static final String FLUSH_DURATION = "kvdb_flush_duration_ms";

    // Gauges
    public static final String MEMTABLE_ACTIVE_SIZE = "kvdb_memtable_active_bytes";
    public static final String MEMTABLE_IMMUTABLE_SIZE = "kvdb_memtable_immutable_bytes";
    public static final String SSTABLE_COUNT = "kvdb_sstable_count";
    public static final String WAL_SIZE = "kvdb_wal_bytes";

    /**
     * 初始化全部指标并绑定 Gauge。
     *
     * @param config 指标配置
     */
    public static void init(MetricsConfig config) {
        MetricRegistry reg = MetricRegistry.getInstance();

        // Counters
        reg.counter(PUT_COUNT, "Total number of put operations");
        reg.counter(GET_COUNT, "Total number of get operations");
        reg.counter(DELETE_COUNT, "Total number of delete operations");
        reg.counter(COMPACTION_COUNT, "Total number of compaction executions");
        reg.counter(FLUSH_COUNT, "Total number of memtable flushes");
        reg.counter(WAL_ROTATE_COUNT, "Total number of WAL segment rotations");
        reg.counter(WAL_CRC_FAILURE_COUNT, "Total number of WAL CRC validation failures");

        // Histograms
        reg.histogram(PUT_LATENCY, "Put operation latency in milliseconds");
        reg.histogram(GET_LATENCY, "Get operation latency in milliseconds");
        reg.histogram(COMPACTION_DURATION, "Compaction duration in milliseconds");
        reg.histogram(FLUSH_DURATION, "MemTable flush duration in milliseconds");

        // Gauges（延迟绑定，避免循环依赖）
        if (config.memTableState() != null) {
            reg.gauge(MEMTABLE_ACTIVE_SIZE, "Active MemTable size in bytes",
                    () -> config.memTableState().getActive().getSize());
            reg.gauge(MEMTABLE_IMMUTABLE_SIZE, "Immutable MemTable size in bytes",
                    () -> {
                        var imm = config.memTableState().getImmutable();
                        return imm != null ? imm.getSize() : 0;
                    });
        }

        if (config.versionSet() != null) {
            reg.gauge(SSTABLE_COUNT, "Total number of SSTable files across all levels",
                    () -> {
                        int total = 0;
                        for (int i = 0; i < config.versionSet().getMaxLevel(); i++) {
                            total += config.versionSet().getLevelSize(i);
                        }
                        return total;
                    });
        }
    }

    /**
     * 指标配置，持有需要绑定 Gauge 的组件引用。
     */
    public record MetricsConfig(
            MemTableState memTableState,
            VersionSet versionSet
    ) {
    }
}
