package client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.BinaryProtocol;
import protocol.BinaryProtocol.ResponseFrame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 二进制协议客户端。
 * <p>
 * 使用 NIO SocketChannel 与服务端通信，支持同步请求-响应模式。
 * 每个请求分配唯一 ReqId，通过阻塞读取等待响应。
 */
public class BinaryClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BinaryClient.class);

    private final String host;
    private final int port;
    private final int timeoutMs;
    private SocketChannel channel;
    private final AtomicLong reqIdGen = new AtomicLong(0);

    public BinaryClient(String host, int port) {
        this(host, port, 5000);
    }

    public BinaryClient(String host, int port, int timeoutMs) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
    }

    /**
     * 连接到服务器。
     */
    public void connect() throws IOException {
        channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.socket().setSoTimeout(timeoutMs);
        channel.connect(new InetSocketAddress(host, port));
        log.info("已连接到 {}:{}", host, port);
    }

    /**
     * 发送 GET 请求。
     *
     * @return value 字节数组，若不存在返回 null
     */
    public byte[] get(byte[] key) throws IOException {
        long reqId = reqIdGen.incrementAndGet();
        ByteBuffer request = BinaryProtocol.buildRequest(BinaryProtocol.CMD_GET, reqId, key, null);
        ResponseFrame response = sendAndReceive(request);
        if (response.status() == BinaryProtocol.STATUS_NOT_FOUND) {
            return null;
        }
        checkStatus(response);
        return response.body() != null && response.body().length > 0 ? response.body() : null;
    }

    /**
     * 发送 PUT 请求。
     */
    public void put(byte[] key, byte[] value) throws IOException {
        long reqId = reqIdGen.incrementAndGet();
        ByteBuffer request = BinaryProtocol.buildRequest(BinaryProtocol.CMD_PUT, reqId, key, value);
        ResponseFrame response = sendAndReceive(request);
        checkStatus(response);
    }

    /**
     * 发送 DELETE 请求。
     */
    public void delete(byte[] key) throws IOException {
        long reqId = reqIdGen.incrementAndGet();
        ByteBuffer request = BinaryProtocol.buildRequest(BinaryProtocol.CMD_DELETE, reqId, key, null);
        ResponseFrame response = sendAndReceive(request);
        checkStatus(response);
    }

    /**
     * 发送 PING 请求。
     *
     * @return true 如果服务器响应 PONG
     */
    public boolean ping() throws IOException {
        long reqId = reqIdGen.incrementAndGet();
        ByteBuffer request = BinaryProtocol.buildRequest(BinaryProtocol.CMD_PING, reqId, null, null);
        ResponseFrame response = sendAndReceive(request);
        return response.status() == BinaryProtocol.STATUS_OK
                && response.body() != null
                && "PONG".equals(new String(response.body()));
    }

    /**
     * 发送 STATS 请求。
     *
     * @return 服务器统计信息
     */
    public String stats() throws IOException {
        long reqId = reqIdGen.incrementAndGet();
        ByteBuffer request = BinaryProtocol.buildRequest(BinaryProtocol.CMD_STATS, reqId, null, null);
        ResponseFrame response = sendAndReceive(request);
        checkStatus(response);
        return response.body() != null ? new String(response.body()) : "";
    }

    /**
     * 发送请求并读取响应。
     */
    private ResponseFrame sendAndReceive(ByteBuffer request) throws IOException {
        if (channel == null || !channel.isOpen()) {
            connect();
        }

        // 发送请求，阻塞模式下仍可能只写入部分数据
        while (request.hasRemaining()) {
            if (channel.write(request) == -1) {
                throw new IOException("连接已关闭");
            }
        }

        // 读取响应头（固定头 12 字节 + 4 字节 bodyLen）
        ByteBuffer headerBuf = ByteBuffer.allocate(BinaryProtocol.RESPONSE_HEADER_SIZE + 4);
        readFully(headerBuf);

        // 解析头部获取 bodyLen
        headerBuf.flip();
        headerBuf.getShort(); // magic
        headerBuf.get();      // version
        headerBuf.get();      // status
        headerBuf.getLong();  // reqId
        int bodyLen = headerBuf.getInt();

        // 读取 body
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen);
        if (bodyLen > 0) {
            readFully(bodyBuf);
        }

        // 组装完整响应（响应头中已经包含 bodyLen，不需要再次写入）
        ByteBuffer fullResponse = ByteBuffer.allocate(BinaryProtocol.RESPONSE_HEADER_SIZE + 4 + bodyLen);
        headerBuf.rewind();
        fullResponse.put(headerBuf);
        bodyBuf.flip();
        fullResponse.put(bodyBuf);
        fullResponse.flip();

        return BinaryProtocol.parseResponse(fullResponse);
    }

    /**
     * 确保读取指定字节数。
     */
    private void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("连接已关闭");
            }
        }
    }

    private void checkStatus(ResponseFrame response) throws IOException {
        if (response.status() == BinaryProtocol.STATUS_ERROR) {
            String msg = response.body() != null ? new String(response.body()) : "未知错误";
            throw new IOException("服务器错误: " + msg);
        }
    }

    /**
     * 断开连接。
     */
    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            channel = null;
        }
    }

    @Override
    public void close() {
        disconnect();
    }

    public boolean isConnected() {
        return channel != null && channel.isOpen();
    }
}
