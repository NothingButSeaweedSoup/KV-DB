package serializer;

import java.io.IOException;

/**
 * 序列化器接口，提供对象与字节数组之间的转换。
 *
 * @param <T> 序列化目标类型
 */
public interface Serializer<T> {

    /**
     * 将对象序列化为字节数组
     *
     * @param value 待序列化的对象，不能为null
     * @return 序列化后的字节数组
     * @throws IOException 序列化失败时抛出
     */
    byte[] serialize(T value) throws IOException;

    /**
     * 将字节数组反序列化为对象
     *
     * @param bytes 字节数组，不能为null
     * @return 反序列化后的对象
     * @throws IOException 反序列化失败时抛出
     */
    T deserialize(byte[] bytes) throws IOException;
}
