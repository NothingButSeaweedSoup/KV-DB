package server;

import core.LSMStorageEngine;
import metrics.MetricRegistry;
import metrics.MetricsHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.BinaryProtocol;
import protocol.BinaryProtocol.RequestFrame;
import protocol.ProtocolException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 基于 NIO Selector 的 Reactor 模型服务器。
 * <p>
 * 架构：
 * <ul>
 *   <li>MainReactor 单线程：接受连接（Accept 事件）</li>
 *   <li>IO 线程：读写事件（Read/Write 事件），解析帧</li>
 *   <li>Business 线程池：执行数据库操作，避免阻塞 IO 线程</li>
 * </ul>
 */
public class NioServer {

    private static final Logger log = LoggerFactory.getLogger(NioServer.class);

    private final int port;
    private final int metricsPort;
    private final LSMStorageEngine engine;
    private final ExecutorService businessPool;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean running;
    private MetricsHttpServer metricsServer;

    public NioServer(int port, LSMStorageEngine engine) {
        this(port, engine, port + 1);
    }

    public NioServer(int port, LSMStorageEngine engine, int metricsPort) {
        this.port = port;
        this.metricsPort = metricsPort;
        this.engine = engine;
        this.businessPool = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "nio-business");
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * 启动服务器。
     */
    public void start() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running = true;

        // 启动 Metrics HTTP 端点
        try {
            metricsServer = new MetricsHttpServer(metricsPort, MetricRegistry.getInstance());
            metricsServer.start();
        } catch (IOException e) {
            log.warn("Metrics HTTP 服务启动失败（端口 {}），监控端点不可用", metricsPort, e);
        }

        log.info("NIO 服务器已启动，端口: {}, Metrics端口: {}", port, metricsPort);

        // 主事件循环
        while (running) {
            selector.select(1000); // 1秒超时，便于优雅关闭
            Set<SelectionKey> keys = selector.selectedKeys();
            Iterator<SelectionKey> iter = keys.iterator();

            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                if (!key.isValid()) {
                    continue;
                }

                try {
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                } catch (IOException e) {
                    log.warn("处理事件异常，关闭连接", e);
                    closeKey(key);
                }
            }
        }

        shutdown();
    }

    /**
     * 停止服务器。
     */
    public void stop() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
    }

    private void shutdown() {
        if (metricsServer != null) {
            metricsServer.stop();
        }
        businessPool.shutdown();
        if (serverChannel != null) {
            try {
                serverChannel.close();
            } catch (IOException ignored) {
            }
        }
        if (selector != null) {
            try {
                selector.close();
            } catch (IOException ignored) {
            }
        }
        log.info("NIO 服务器已停止");
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel client = ssc.accept();
        if (client == null) {
            return;
        }
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ, new ClientState());
        log.debug("新连接: {}", client.getRemoteAddress());
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();

        ByteBuffer readBuf = state.readBuffer;
        int bytesRead = channel.read(readBuf);

        if (bytesRead == -1) {
            closeKey(key);
            return;
        }

        // 尝试解析完整帧
        readBuf.flip();
        try {
            while (readBuf.remaining() >= BinaryProtocol.REQUEST_HEADER_SIZE) {
                readBuf.mark();

                // 预读帧头以计算完整帧长度
                short magic = readBuf.getShort();
                if (magic != BinaryProtocol.MAGIC) {
                    log.warn("收到无效魔数，关闭连接");
                    closeKey(key);
                    return;
                }
                byte version = readBuf.get();
                if (version != BinaryProtocol.VERSION) {
                    log.warn("不支持的协议版本: {}，关闭连接", version);
                    closeKey(key);
                    return;
                }
                readBuf.get(); // cmd
                readBuf.getLong(); // reqId
                int keyLen = readBuf.getInt();
                if (keyLen < 0 || readBuf.remaining() < keyLen + 4) {
                    readBuf.reset();
                    break;
                }
                readBuf.position(readBuf.position() + keyLen);
                int valLen = readBuf.getInt();
                if (valLen < 0) {
                    log.warn("收到非法的负值长度: {}，关闭连接", valLen);
                    closeKey(key);
                    return;
                }

                readBuf.reset();
                int frameSize = BinaryProtocol.REQUEST_HEADER_SIZE + 4 + keyLen + 4 + valLen;
                if (frameSize > BinaryProtocol.MAX_FRAME_SIZE) {
                    log.warn("帧大小 {} 超过最大值 {}，关闭连接", frameSize, BinaryProtocol.MAX_FRAME_SIZE);
                    closeKey(key);
                    return;
                }
                if (readBuf.remaining() < frameSize) {
                    break;
                }

                // 截取完整帧，保证读缓冲区中后续数据不丢失
                ByteBuffer frame = ByteBuffer.allocate(frameSize);
                int oldLimit = readBuf.limit();
                readBuf.limit(readBuf.position() + frameSize);
                frame.put(readBuf);
                readBuf.limit(oldLimit);
                frame.flip();

                RequestFrame request;
                try {
                    request = BinaryProtocol.parseRequest(frame);
                } catch (ProtocolException e) {
                    log.warn("帧解析失败，关闭连接", e);
                    closeKey(key);
                    return;
                }

                final SelectionKey finalKey = key;
                businessPool.submit(() -> handleRequest(finalKey, request));
            }
        } finally {
            // 将未解析的数据移到缓冲区前端，等待下一次读取
            readBuf.compact();
        }
    }

    /**
     * 处理请求并发送响应。
     */
    private void handleRequest(SelectionKey key, RequestFrame request) {
        try {
            Response response = processRequest(request);
            ByteBuffer respBuf = BinaryProtocol.buildResponse(
                    response.status(), request.reqId(), response.body());
            queueWrite(key, respBuf);
        } catch (Exception e) {
            log.error("处理请求异常", e);
            String msg = e.getMessage() != null ? e.getMessage() : "Internal error";
            ByteBuffer errBuf = BinaryProtocol.buildResponse(
                    BinaryProtocol.STATUS_ERROR, request.reqId(), msg.getBytes(StandardCharsets.UTF_8));
            queueWrite(key, errBuf);
        }
    }

    private record Response(byte status, byte[] body) {
    }

    /**
     * 处理请求，返回响应状态与 body。
     */
    private Response processRequest(RequestFrame request) throws IOException, ClassNotFoundException {
        return switch (request.cmd()) {
            case BinaryProtocol.CMD_GET -> {
                Object result = engine.get(request.key());
                if (result == null) {
                    yield new Response(BinaryProtocol.STATUS_NOT_FOUND, null);
                }
                yield new Response(BinaryProtocol.STATUS_OK, valueToBytes(result));
            }
            case BinaryProtocol.CMD_PUT -> {
                engine.put(request.key(), request.value());
                yield new Response(BinaryProtocol.STATUS_OK, "OK".getBytes(StandardCharsets.UTF_8));
            }
            case BinaryProtocol.CMD_DELETE -> {
                engine.delete(request.key());
                yield new Response(BinaryProtocol.STATUS_OK, "OK".getBytes(StandardCharsets.UTF_8));
            }
            case BinaryProtocol.CMD_PING -> new Response(BinaryProtocol.STATUS_OK, "PONG".getBytes(StandardCharsets.UTF_8));
            case BinaryProtocol.CMD_STATS -> new Response(BinaryProtocol.STATUS_OK, getStats().getBytes(StandardCharsets.UTF_8));
            default -> new Response(BinaryProtocol.STATUS_ERROR,
                    ("UNKNOWN_CMD: 0x" + Integer.toHexString(request.cmd() & 0xFF)).getBytes(StandardCharsets.UTF_8));
        };
    }

    private byte[] valueToBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String getStats() {
        var reg = metrics.MetricRegistry.getInstance();
        var counters = reg.getCounters();
        var sb = new StringBuilder();
        sb.append("put_total=").append(counters.getOrDefault(metrics.Metrics.PUT_COUNT, new metrics.Counter("", "")).get()).append("\n");
        sb.append("get_total=").append(counters.getOrDefault(metrics.Metrics.GET_COUNT, new metrics.Counter("", "")).get()).append("\n");
        sb.append("delete_total=").append(counters.getOrDefault(metrics.Metrics.DELETE_COUNT, new metrics.Counter("", "")).get()).append("\n");
        sb.append("compaction_total=").append(counters.getOrDefault(metrics.Metrics.COMPACTION_COUNT, new metrics.Counter("", "")).get()).append("\n");
        sb.append("flush_total=").append(counters.getOrDefault(metrics.Metrics.FLUSH_COUNT, new metrics.Counter("", "")).get()).append("\n");
        return sb.toString();
    }

    private void handleWrite(SelectionKey key) throws IOException {
        ClientState state = (ClientState) key.attachment();
        ByteBuffer writeBuf = state.writeBuffer;

        if (writeBuf == null || !writeBuf.hasRemaining()) {
            // 写队列为空或当前 buffer 已写完
            synchronized (state.writeQueue) {
                state.writeBuffer = state.writeQueue.poll();
                if (state.writeBuffer == null) {
                    // 队列为空，取消写兴趣
                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                    return;
                }
            }
        }

        SocketChannel channel = (SocketChannel) key.channel();
        channel.write(state.writeBuffer);

        if (!state.writeBuffer.hasRemaining()) {
            synchronized (state.writeQueue) {
                state.writeBuffer = state.writeQueue.poll();
                if (state.writeBuffer == null) {
                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                }
            }
        }
    }

    /**
     * 将响应 buffer 加入写队列，并注册写兴趣。
     */
    private void queueWrite(SelectionKey key, ByteBuffer buf) {
        if (!key.isValid()) {
            return;
        }
        ClientState state = (ClientState) key.attachment();
        synchronized (state.writeQueue) {
            state.writeQueue.offer(buf);
        }
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    private void closeKey(SelectionKey key) {
        try {
            key.channel().close();
        } catch (IOException ignored) {
        }
        key.cancel();
    }

    /**
     * 每个客户端连接的状态。
     */
    private static class ClientState {
        /** 读缓冲区：积累不完整的帧数据 */
        final ByteBuffer readBuffer = ByteBuffer.allocate(64 * 1024);
        /** 当前正在写的 buffer */
        volatile ByteBuffer writeBuffer;
        /** 写队列：多个响应排队发送 */
        final java.util.Queue<ByteBuffer> writeQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();
    }
}
