package cluster;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * ReplicationMessage 自定义二进制序列化器。
 * <p>
 * 替代 Java 原生 ObjectOutputStream，格式更紧凑、速度更快、支持跨语言解析。
 * <p>
 * 帧格式：
 * <pre>
 * Seq(8B) | Op(1B) | KeyLen(4B) | Key | ValLen(4B) | Value
 * </pre>
 * 其中 Op: 0x01=PUT, 0x02=DELETE。Value 仅在 PUT 时存在，DELETE 时 ValLen=0。
 */
public final class ReplicationCodec {

    private static final byte OP_PUT = 0x01;
    private static final byte OP_DELETE = 0x02;

    private ReplicationCodec() {
    }

    /**
     * 将 ReplicationMessage 序列化为字节数组。
     */
    public static byte[] encode(ReplicationMessage msg) {
        byte[] key = msg.getKey();
        byte[] valueBytes = serializeValue(msg.getValue());
        int valLen = valueBytes != null ? valueBytes.length : 0;

        int totalSize = 8 + 1 + 4 + key.length + 4 + valLen;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putLong(msg.getSeq());
        buf.put(msg.getOp() == ReplicationMessage.Operation.PUT ? OP_PUT : OP_DELETE);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(valLen);
        if (valLen > 0) {
            buf.put(valueBytes);
        }
        return buf.array();
    }

    /**
     * 从字节数组反序列化为 ReplicationMessage。
     */
    public static ReplicationMessage decode(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        long seq = buf.getLong();
        byte opByte = buf.get();
        ReplicationMessage.Operation op = (opByte == OP_PUT)
                ? ReplicationMessage.Operation.PUT
                : ReplicationMessage.Operation.DELETE;

        int keyLen = buf.getInt();
        byte[] key = new byte[keyLen];
        buf.get(key);

        int valLen = buf.getInt();
        Object value = null;
        if (valLen > 0) {
            byte[] valueBytes = new byte[valLen];
            buf.get(valueBytes);
            value = deserializeValue(valueBytes);
        }

        return new ReplicationMessage(seq, op, key, value);
    }

    /**
     * 将 value 对象序列化为字节数组。
     * 支持 String 类型的高效序列化，其他类型回退到 Java 序列化。
     */
    private static byte[] serializeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        // 回退到 Java 序列化
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("序列化 value 失败", e);
        }
    }

    /**
     * 从字节数组反序列化 value 对象。
     * 先尝试 UTF-8 解码为 String，失败则回退到 Java 反序列化。
     */
    private static Object deserializeValue(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        // 尝试 Java 反序列化（因为写入端可能是 ObjectOutputStream）
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        } catch (Exception e) {
            // 回退为 UTF-8 字符串
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
