package serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * JSON序列化器，基于Jackson实现。
 * <p>
 * 支持任意可JSON序列化的对象类型。
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Class<T> type;

    /**
     * 创建指定类型的JSON序列化器
     *
     * @param type 目标类型的Class对象
     */
    public JsonSerializer(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("type不能为null");
        }
        this.type = type;
    }

    @Override
    public byte[] serialize(T value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("value不能为null");
        }
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IOException("JSON序列化失败: " + e.getMessage(), e);
        }
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes不能为null");
        }
        try {
            return MAPPER.readValue(bytes, type);
        } catch (JsonProcessingException e) {
            throw new IOException("JSON反序列化失败: " + e.getMessage(), e);
        }
    }
}
