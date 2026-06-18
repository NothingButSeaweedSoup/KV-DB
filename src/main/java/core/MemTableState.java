package core;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 双缓冲 MemTable 管理器。
 * <p>
 * 维护一个活跃（active）MemTable 接收写入，和一个可选的不可变（immutable）MemTable
 * 等待后台 flush 到 SSTable。当 active 达到阈值时，通过 CAS 原子切换为 immutable，
 * 并创建新的 active，整个过程不阻塞读写。
 */
public class MemTableState {

    private final AtomicReference<State> stateRef;

    public MemTableState(MemTable active) {
        this.stateRef = new AtomicReference<>(new State(active, null, 0));
    }

    /**
     * 获取当前活跃的 MemTable。
     */
    public MemTable getActive() {
        return stateRef.get().active;
    }

    /**
     * 获取当前不可变的 MemTable，可能为 null。
     */
    public MemTable getImmutable() {
        return stateRef.get().immutable;
    }

    /**
     * 获取当前版本号。
     */
    public long getVersion() {
        return stateRef.get().version;
    }

    /**
     * 检查 active 是否已满。
     */
    public boolean isActiveFull() {
        return stateRef.get().active.isFull();
    }

    /**
     * 检查是否存在正在 flush 的不可变 MemTable。
     */
    public boolean hasImmutable() {
        return stateRef.get().immutable != null;
    }

    /**
     * 将当前 active 切换为 immutable，并创建新的 active。
     * 使用 CAS 保证原子性，仅在没有 immutable 时允许切换。
     *
     * @param newActive 新的活跃 MemTable
     * @return 切换成功返回 true，说明新的 immutable 需要 flush；若已有 immutable 则返回 false
     */
    public boolean switchActive(MemTable newActive) {
        State current = stateRef.get();
        if (current.immutable != null) {
            // 已有 immutable 正在 flush，不允许切换
            return false;
        }
        // 先 freeze 再 CAS：freeze 是幂等的（设 volatile true），CAS 失败不会产生副作用
        current.active.freeze();
        State next = new State(newActive, current.active, current.version + 1);
        if (stateRef.compareAndSet(current, next)) {
            return true;
        }
        // CAS 失败：另一个线程已完成切换，撤销 freeze（不影响正确性，仅恢复可写性）
        current.active.unfreeze();
        return false;
    }

    /**
     * 清除已 flush 完成的 immutable MemTable。
     * 在后台 flush 完成后调用。
     */
    public void clearImmutable() {
        State current;
        State next;
        do {
            current = stateRef.get();
            if (current.immutable == null) {
                return;
            }
            next = new State(current.active, null, current.version);
        } while (!stateRef.compareAndSet(current, next));
    }

    /**
     * 从 active 和 immutable（若存在）中查询 key。
     *
     * @return value 字节数组，或 null 表示不存在
     */
    public byte[] get(byte[] key) {
        State state = stateRef.get();
        byte[] value = state.active.get(key);
        if (value != null) {
            return value;
        }
        if (state.immutable != null) {
            return state.immutable.get(key);
        }
        return null;
    }

    /**
     * 获取 active 和 immutable 中所有数据的合并快照（active 优先覆盖 immutable）。
     * 用于 flush 操作。
     */
    public Map<byte[], byte[]> getActiveSnapshot() {
        return stateRef.get().active.snapshot();
    }

    private record State(MemTable active, MemTable immutable, long version) {
    }
}
