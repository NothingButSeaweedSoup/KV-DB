package metrics;

/**
 * 瞬时值指标接口。
 * 用于暴露当前 MemTable 大小、SSTable 数量等实时状态。
 */
public interface Gauge {

    /**
     * 获取当前值。
     */
    double getValue();
}
