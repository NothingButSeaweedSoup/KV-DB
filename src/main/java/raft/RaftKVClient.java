package raft;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Raft KV 客户端封装。通过 Ratis RaftClient 发送 put/get/delete 请求。
 * <p>
 * 写请求（put/delete）通过 Raft 日志复制；
 * 读请求（get）通过只读查询路径直接读 Leader 状态机。
 * <p>
 * 内置自动重试：当 Leader 切换或网络抖动时，自动重试请求。
 * Ratis RaftClient 内置 Leader 跟踪和 NotLeaderException 重定向机制。
 */
public class RaftKVClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftKVClient.class);

    /** 默认最大重试次数 */
    private static final int DEFAULT_MAX_RETRIES = 5;
    /** 默认重试间隔（毫秒） */
    private static final long DEFAULT_RETRY_INTERVAL_MS = 200;

    private final RaftClient client;
    private final RaftGroup group;
    private final int maxRetries;
    private final long retryIntervalMs;

    /**
     * 使用 RaftConfig 构建客户端。连接到配置中的所有 peer。
     */
    public RaftKVClient(RaftConfig config) {
        this(config, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL_MS);
    }

    /**
     * 使用 RaftConfig 构建客户端，可配置重试参数。
     */
    public RaftKVClient(RaftConfig config, int maxRetries, long retryIntervalMs) {
        this.group = buildGroup(config);
        this.client = buildClient(this.group);
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
    }

    /**
     * 使用 RaftGroup 构建客户端（用于测试或高级场景）。
     */
    public RaftKVClient(RaftGroup group) {
        this(group, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL_MS);
    }

    /**
     * 使用 RaftGroup 构建客户端，可配置重试参数。
     */
    public RaftKVClient(RaftGroup group, int maxRetries, long retryIntervalMs) {
        this.group = group;
        this.client = buildClient(group);
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
    }

    private RaftGroup buildGroup(RaftConfig config) {
        RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(config.raftGroupId()));
        List<RaftPeer> peers = config.peers().stream()
                .map(p -> RaftPeer.newBuilder()
                        .setId(RaftPeerId.valueOf(p.id()))
                        .setAddress(p.address())
                        .build())
                .collect(Collectors.toList());
        return RaftGroup.valueOf(groupId, peers);
    }

    private RaftClient buildClient(RaftGroup group) {
        RaftProperties props = new RaftProperties();
        RaftConfigKeys.Rpc.setType(props, SupportedRpcType.NETTY);
        return RaftClient.newBuilder()
                .setProperties(props)
                .setRaftGroup(group)
                .build();
    }

    /**
     * 写入键值对。请求通过 Raft 日志复制到多数派后返回。
     * 内置重试：Leader 切换时自动重试。
     *
     * @param key   键
     * @param value 值（会被 JSON 序列化）
     * @throws IOException 如果写入失败（重试耗尽后）
     */
    public void put(byte[] key, Object value) throws IOException {
        withRetry(() -> {
            Command cmd = new Command(Command.CommandType.PUT, key, value);
            byte[] encoded = CommandCodec.encode(cmd);
            RaftClientReply reply = client.io().send(Message.valueOf(ByteString.copyFrom(encoded)));
            if (!reply.isSuccess()) {
                throw new IOException("Put failed: " + reply.getException());
            }
        }, "put");
    }

    /**
     * 删除键。请求通过 Raft 日志复制到多数派后返回。
     * 内置重试：Leader 切换时自动重试。
     *
     * @param key 键
     * @throws IOException 如果删除失败（重试耗尽后）
     */
    public void delete(byte[] key) throws IOException {
        withRetry(() -> {
            Command cmd = new Command(Command.CommandType.DELETE, key, null);
            byte[] encoded = CommandCodec.encode(cmd);
            RaftClientReply reply = client.io().send(Message.valueOf(ByteString.copyFrom(encoded)));
            if (!reply.isSuccess()) {
                throw new IOException("Delete failed: " + reply.getException());
            }
        }, "delete");
    }

    /**
     * 读取值。通过只读查询路径直接读 Leader 状态机。
     * 内置重试：Leader 切换时自动重试。
     *
     * @param key 键
     * @return 值，如果不存在返回 null
     * @throws IOException 如果读取失败（重试耗尽后）
     */
    public Object get(byte[] key) throws IOException {
        return withRetryAndReturn(() -> {
            byte[] req = CommandCodec.encodeGetRequest(key);
            RaftClientReply reply = client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(req)));
            if (!reply.isSuccess()) {
                throw new IOException("Get failed: " + reply.getException());
            }
            return CommandCodec.decodeGetResponse(reply.getMessage().getContent().toByteArray());
        }, "get");
    }

    /**
     * 获取底层 RaftClient（供高级用途）。
     */
    public RaftClient getRaftClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    // ======================== 重试逻辑 ========================

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }

    private void withRetry(IoAction action, String operationName) throws IOException {
        IOException lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                action.run();
                return;
            } catch (IOException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    log.warn("{} 操作失败 (attempt {}/{}): {}，{}ms 后重试",
                            operationName, attempt + 1, maxRetries + 1, e.getMessage(), retryIntervalMs);
                    try {
                        Thread.sleep(retryIntervalMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("重试被中断", ie);
                    }
                }
            }
        }
        throw new IOException(operationName + " 操作在 " + (maxRetries + 1) + " 次尝试后仍然失败", lastException);
    }

    private <T> T withRetryAndReturn(IoSupplier<T> supplier, String operationName) throws IOException {
        IOException lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return supplier.get();
            } catch (IOException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    log.warn("{} 操作失败 (attempt {}/{}): {}，{}ms 后重试",
                            operationName, attempt + 1, maxRetries + 1, e.getMessage(), retryIntervalMs);
                    try {
                        Thread.sleep(retryIntervalMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("重试被中断", ie);
                    }
                }
            }
        }
        throw new IOException(operationName + " 操作在 " + (maxRetries + 1) + " 次尝试后仍然失败", lastException);
    }
}
