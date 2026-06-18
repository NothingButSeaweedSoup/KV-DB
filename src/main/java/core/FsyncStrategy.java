package core;

/**
 * WAL fsync 策略枚举。
 * <ul>
 *   <li>{@link #SYNC} - 每次写入后立即fsync，强一致，低吞吐</li>
 *   <li>{@link #BATCH} - 批量fsync（默认），平衡性能与一致性</li>
 *   <li>{@link #ASYNC} - 后台定时fsync，最高吞吐，可接受秒级丢失</li>
 * </ul>
 */
public enum FsyncStrategy {

    /**
     * 每次写入后调用 channel.force(true)，保证数据不丢失。
     * 适用场景：对数据安全性要求极高的场景。
     */
    SYNC,

    /**
     * 批量写入后调用 channel.force(true)，默认策略。
     * 每 {@code batchInterval} 条记录或每 {@code batchIntervalMs} 毫秒触发一次 fsync。
     * 适用场景：大多数业务场景，平衡性能与一致性。
     */
    BATCH,

    /**
     * 后台线程定期调用 channel.force(true)，最高吞吐。
     * 可接受秒级数据丢失风险。
     * 适用场景：日志采集、监控数据等可容忍少量丢失的场景。
     */
    ASYNC
}
