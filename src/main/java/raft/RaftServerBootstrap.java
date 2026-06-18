package raft;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Ratis 服务端启动器。根据 {@link RaftConfig} 构建并启动 RaftServer。
 */
public class RaftServerBootstrap implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftServerBootstrap.class);

    private final RaftServer server;
    private final RaftConfig config;

    public RaftServerBootstrap(RaftConfig config) throws IOException {
        this.config = config;
        this.server = buildServer(config);
    }

    private RaftServer buildServer(RaftConfig config) throws IOException {
        // 1. 创建 RaftProperties
        RaftProperties properties = new RaftProperties();

        // 设置 RPC 类型为 Netty
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);

        // 设置存储目录
        File storageDir = new File(config.dataDir(), config.nodeId());
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        // 设置 RPC 端口（使用 Netty 传输层）
        NettyConfigKeys.Server.setPort(properties, config.rpcPort());

        // 设置心跳与选举超时
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
                TimeDuration.valueOf(config.electionTimeoutMinMs(), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
                TimeDuration.valueOf(config.electionTimeoutMaxMs(), TimeUnit.MILLISECONDS));

        // 设置快照
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, config.snapshotThreshold());

        // 2. 构建 RaftGroup
        RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(config.raftGroupId()));
        List<RaftPeer> peers = config.peers().stream()
                .map(p -> RaftPeer.newBuilder()
                        .setId(RaftPeerId.valueOf(p.id()))
                        .setAddress(p.address())
                        .build())
                .collect(Collectors.toList());
        RaftGroup group = RaftGroup.valueOf(groupId, peers);

        // 3. 判断启动选项
        boolean hasExistingStorage = storageDir.exists() && storageDir.isDirectory()
                && storageDir.list() != null && storageDir.list().length > 0;
        RaftStorage.StartupOption startupOption = hasExistingStorage
                ? RaftStorage.StartupOption.RECOVER
                : RaftStorage.StartupOption.FORMAT;

        // 4. 构建 RaftServer
        RaftPeerId selfId = RaftPeerId.valueOf(config.nodeId());

        log.info("构建 Ratis 服务端: nodeId={}, address={}, dataDir={}, startup={}",
                config.nodeId(), config.getSelfAddress(), storageDir.getAbsolutePath(), startupOption);

        return RaftServer.newBuilder()
                .setServerId(selfId)
                .setGroup(group)
                .setProperties(properties)
                .setStateMachine(new KvStoreStateMachine())
                .setOption(startupOption)
                .build();
    }

    /**
     * 启动 Ratis 服务端。
     */
    public void start() throws IOException {
        server.start();
        log.info("Ratis 服务端已启动: nodeId={}, address={}", config.nodeId(), config.getSelfAddress());
    }

    /**
     * 获取底层 RaftServer（供高级用途）。
     */
    public RaftServer getServer() {
        return server;
    }

    /**
     * 获取配置。
     */
    public RaftConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            log.info("关闭 Ratis 服务端: nodeId={}", config.nodeId());
            server.close();
        }
    }
}
