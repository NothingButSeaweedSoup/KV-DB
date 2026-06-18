package serializer;

import java.nio.charset.StandardCharsets;

/**
 * 字符串序列化器，使用UTF-8编码进行编解码。
 */
public class StringSerializer implements Serializer<String> {

    public static final StringSerializer INSTANCE = new StringSerializer();

    @Override
    public byte[] serialize(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value不能为null");
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes不能为null");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
