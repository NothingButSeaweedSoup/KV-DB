package serializer;

import java.io.*;

/**
 * Java原生序列化器，保留原有行为作为兼容选项。
 * <p>
 * 目标类型必须实现 {@link java.io.Serializable} 接口。
 *
 * @param <T> 序列化目标类型
 */
public class JavaSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(T value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("value不能为null");
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Java反序列化失败: " + e.getMessage(), e);
        }
    }
}
