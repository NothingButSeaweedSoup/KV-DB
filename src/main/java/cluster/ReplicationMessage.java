package cluster;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public final class ReplicationMessage implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public enum Operation {
        PUT,
        DELETE
    }

    private final long seq;
    private final Operation op;
    private final byte[] key;
    private final Object value;

    public ReplicationMessage(long seq, Operation op, byte[] key, Object value) {
        this.seq = seq;
        this.op = Objects.requireNonNull(op, "op");
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
    }

    public long getSeq() {
        return seq;
    }

    public Operation getOp() {
        return op;
    }

    public byte[] getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ReplicationMessage{" +
                "seq=" + seq +
                ", op=" + op +
                ", key=" + Arrays.toString(key) +
                ", value=" + value +
                '}';
    }
}
