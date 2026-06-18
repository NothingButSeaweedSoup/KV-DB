package serializer;

/**
 * 字节数组序列化器，直接透传字节数组。
 */
public class BytesSerializer implements Serializer<byte[]> {

    public static final BytesSerializer INSTANCE = new BytesSerializer();

    @Override
    public byte[] serialize(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value不能为null");
        }
        return value;
    }

    @Override
    public byte[] deserialize(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes不能为null");
        }
        return bytes;
    }
}
