package raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 命令编解码器，使用简单的二进制协议：<br>
 * <pre>
 * 写命令（PUT）: type(1B) | keyLen(4B) | key | valueLen(4B) | value(JSON)
 * 写命令（DELETE）: type(1B) | keyLen(4B) | key
 * 写命令（FLUSH）: type(1B)
 * GET 请求: keyLen(4B) | key
 * GET 响应: found(1B) | [valueLen(4B) | value(JSON)]
 * </pre>
 */
public class CommandCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ======================== 写命令编解码（用于 Raft 日志） ========================

    /**
     * 将 Command 编码为字节数组，用于写入 Raft 日志。
     */
    public static byte[] encode(Command cmd) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            // 1. type
            out.writeByte(cmd.type().code());

            // 2. key（PUT / DELETE 都有 key）
            if (cmd.type() != Command.CommandType.FLUSH) {
                out.writeInt(cmd.key().length);
                out.write(cmd.key());
            }

            // 3. value（只有 PUT 才有 value）
            if (cmd.type() == Command.CommandType.PUT) {
                byte[] valueBytes = MAPPER.writeValueAsBytes(cmd.value());
                out.writeInt(valueBytes.length);
                out.write(valueBytes);
            }

            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode command", e);
        }
    }

    /**
     * 从字节数组解码出 Command。
     */
    public static Command decode(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

            // 1. type
            Command.CommandType type = Command.CommandType.of(in.readByte());

            // 2. key
            byte[] key = null;
            if (type != Command.CommandType.FLUSH) {
                int keyLen = in.readInt();
                key = new byte[keyLen];
                in.readFully(key);
            }

            // 3. value
            Object value = null;
            if (type == Command.CommandType.PUT) {
                int valueLen = in.readInt();
                byte[] valueBytes = new byte[valueLen];
                in.readFully(valueBytes);
                value = MAPPER.readValue(valueBytes, Object.class);
            }

            return new Command(type, key, value);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode command", e);
        }
    }

    // ======================== GET 请求编解码（走只读查询路径） ========================

    /**
     * 编码 GET 请求：keyLen(4B) | key
     */
    public static byte[] encodeGetRequest(byte[] key) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(key.length);
            out.write(key);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode GET request", e);
        }
    }

    /**
     * 解码 GET 请求，返回 key。
     */
    public static byte[] decodeGetRequest(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            int keyLen = in.readInt();
            byte[] key = new byte[keyLen];
            in.readFully(key);
            return key;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode GET request", e);
        }
    }

    // ======================== GET 响应编解码 ========================

    /**
     * 编码 GET 响应：found(1B) | [valueLen(4B) | value(JSON)]
     */
    public static byte[] encodeGetResponse(Object value) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            if (value == null) {
                out.writeByte(0); // not found
            } else {
                out.writeByte(1); // found
                byte[] valueBytes = MAPPER.writeValueAsBytes(value);
                out.writeInt(valueBytes.length);
                out.write(valueBytes);
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode GET response", e);
        }
    }

    /**
     * 解码 GET 响应。返回 null 表示 key 不存在。
     */
    public static Object decodeGetResponse(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            int found = in.readByte();
            if (found == 0) {
                return null;
            }
            int valueLen = in.readInt();
            byte[] valueBytes = new byte[valueLen];
            in.readFully(valueBytes);
            return MAPPER.readValue(valueBytes, Object.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode GET response", e);
        }
    }
}
