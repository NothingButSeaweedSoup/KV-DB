package protocol;

import java.nio.ByteBuffer;

/**
 * 二进制帧协议定义。
 * <p>
 * 请求帧格式：
 * <pre>
 * Magic(2B) | Version(1B) | Cmd(1B) | ReqId(8B) | KeyLen(4B) | Key | ValLen(4B) | Value
 * </pre>
 * 响应帧格式：
 * <pre>
 * Magic(2B) | Version(1B) | Status(1B) | ReqId(8B) | BodyLen(4B) | Body
 * </pre>
 */
public final class BinaryProtocol {

    private BinaryProtocol() {
    }

    // ---- 协议常量 ----

    /** 魔数：0x4B56 ("KV") */
    public static final short MAGIC = (short) 0x4B56;
    /** 协议版本 */
    public static final byte VERSION = 1;
    /** 请求头固定长度：Magic(2) + Version(1) + Cmd(1) + ReqId(8) = 12 字节 */
    public static final int REQUEST_HEADER_SIZE = 12;
    /** 响应头固定长度：Magic(2) + Version(1) + Status(1) + ReqId(8) = 12 字节 */
    public static final int RESPONSE_HEADER_SIZE = 12;
    /** 最大帧大小：16MB */
    public static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    // ---- 命令类型 ----

    public static final byte CMD_GET = 0x01;
    public static final byte CMD_PUT = 0x02;
    public static final byte CMD_DELETE = 0x03;
    public static final byte CMD_BATCH_GET = 0x11;
    public static final byte CMD_BATCH_PUT = 0x12;
    public static final byte CMD_PING = 0x20;
    public static final byte CMD_STATS = 0x21;

    // ---- 状态码 ----

    public static final byte STATUS_OK = 0x00;
    public static final byte STATUS_NOT_FOUND = 0x01;
    public static final byte STATUS_ERROR = 0x02;

    // ---- 帧构建工具 ----

    /**
     * 构建请求帧。
     */
    public static ByteBuffer buildRequest(byte cmd, long reqId, byte[] key, byte[] value) {
        int keyLen = key != null ? key.length : 0;
        int valLen = value != null ? value.length : 0;
        int frameSize = REQUEST_HEADER_SIZE + 4 + keyLen + 4 + valLen;
        ByteBuffer buf = ByteBuffer.allocate(frameSize);
        buf.putShort(MAGIC);
        buf.put(VERSION);
        buf.put(cmd);
        buf.putLong(reqId);
        buf.putInt(keyLen);
        if (keyLen > 0) {
            buf.put(key);
        }
        buf.putInt(valLen);
        if (valLen > 0) {
            buf.put(value);
        }
        buf.flip();
        return buf;
    }

    /**
     * 构建响应帧。
     */
    public static ByteBuffer buildResponse(byte status, long reqId, byte[] body) {
        int bodyLen = body != null ? body.length : 0;
        int frameSize = RESPONSE_HEADER_SIZE + 4 + bodyLen;
        ByteBuffer buf = ByteBuffer.allocate(frameSize);
        buf.putShort(MAGIC);
        buf.put(VERSION);
        buf.put(status);
        buf.putLong(reqId);
        buf.putInt(bodyLen);
        if (bodyLen > 0) {
            buf.put(body);
        }
        buf.flip();
        return buf;
    }

    // ---- 帧解析 ----

    /**
     * 请求帧解析结果。
     */
    public record RequestFrame(byte cmd, long reqId, byte[] key, byte[] value) {
    }

    /**
     * 响应帧解析结果。
     */
    public record ResponseFrame(byte status, long reqId, byte[] body) {
    }

    /**
     * 从 ByteBuffer 解析请求帧。调用前确保 buffer 已 flip 到读模式。
     */
    public static RequestFrame parseRequest(ByteBuffer buf) {
        short magic = buf.getShort();
        if (magic != MAGIC) {
            throw new ProtocolException("无效的魔数: 0x" + Integer.toHexString(magic & 0xFFFF));
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw new ProtocolException("不支持的协议版本: " + version);
        }
        byte cmd = buf.get();
        long reqId = buf.getLong();
        int keyLen = buf.getInt();
        byte[] key = null;
        if (keyLen > 0) {
            key = new byte[keyLen];
            buf.get(key);
        }
        int valLen = buf.getInt();
        byte[] value = null;
        if (valLen > 0) {
            value = new byte[valLen];
            buf.get(value);
        }
        return new RequestFrame(cmd, reqId, key, value);
    }

    /**
     * 从 ByteBuffer 解析响应帧。
     */
    public static ResponseFrame parseResponse(ByteBuffer buf) {
        short magic = buf.getShort();
        if (magic != MAGIC) {
            throw new ProtocolException("无效的魔数: 0x" + Integer.toHexString(magic & 0xFFFF));
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw new ProtocolException("不支持的协议版本: " + version);
        }
        byte status = buf.get();
        long reqId = buf.getLong();
        int bodyLen = buf.getInt();
        byte[] body = null;
        if (bodyLen > 0) {
            body = new byte[bodyLen];
            buf.get(body);
        }
        return new ResponseFrame(status, reqId, body);
    }
}
