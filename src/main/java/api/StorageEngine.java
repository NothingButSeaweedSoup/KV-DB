package api;

import java.io.IOException;
import java.lang.reflect.Type;

public interface StorageEngine extends AutoCloseable {

    /**
     * 存储键值对
     * 如果键已存在，则更新对应的值
     *
     * @param key   键，不能为null
     * @param value 值，不能为null
     * @throws IOException              当IO操作失败时抛出
     * @throws IllegalArgumentException 当key或value为null时抛出
     */
    void put(byte[] key, Object value) throws IOException;

    /**
     * 获取键对应的值
     * 如果键不存在，返回null
     *
     * @param key 键，不能为null
     * @return 值，如果不存在返回null
     * @throws IOException              当IO操作失败时抛出
     * @throws IllegalArgumentException 当key为null时抛出
     */
    Object get(byte[] key) throws IOException, ClassNotFoundException;

    /**
     * 删除键值对
     * 如果键不存在，则不做任何操作
     *
     * @param key 键，不能为null
     * @throws IOException              当IO操作失败时抛出
     * @throws IllegalArgumentException 当key为null时抛出
     */
    void delete(byte[] key) throws IOException;

    /**
     * 将内存中的数据刷新到磁盘
     * 确保所有未持久化的数据都被写入磁盘
     *
     * @throws IOException 当IO操作失败时抛出
     */
    void flush() throws IOException;

    /**
     * 关闭存储引擎
     * 释放所有资源，确保数据被正确持久化
     * 关闭后，引擎实例将不可用
     *
     * @throws IOException 当IO操作失败时抛出
     */
    void close() throws IOException;
}