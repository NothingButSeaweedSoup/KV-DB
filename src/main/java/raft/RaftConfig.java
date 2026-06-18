package raft;

import java.util.Collections;
import java.util.List;

/**
 * Raft 集群配置模型。通过 {@link RaftConfigLoader} 从 raft-config.json 加载。
 *
 * @param nodeId              本节点 ID，如 "node1"
 * @param raftGroupId         Raft 组 ID（UUID 字符串）
 * @param dataDir             数据目录
 * @param peers               所有 peer 列表
 * @param rpcPort             本节点 RPC 端口
 * @param heartbeatIntervalMs Leader 心跳间隔（毫秒）
 * @param electionTimeoutMinMs 选举超时下限（毫秒）
 * @param electionTimeoutMaxMs 选举超时上限（毫秒）
 * @param snapshotThreshold   自动快照触发的日志条数
 * @param logSegmentSizeMax   单个日志段最大大小（字节）
 */
public record RaftConfig(
        String nodeId,
        String raftGroupId,
        String dataDir,
        List<PeerConfig> peers,
        int rpcPort,
        int heartbeatIntervalMs,
        int electionTimeoutMinMs,
        int electionTimeoutMaxMs,
        int snapshotThreshold,
        long logSegmentSizeMax
) {
    /**
     * 单个 peer 的配置。
     */
    public record PeerConfig(String id, String address) {}

    /** 默认 Raft 组 ID */
    public static final String DEFAULT_GROUP_ID = "02511d47-d67c-49a3-9011-abb3109a44c1";
    /** 默认数据目录 */
    public static final String DEFAULT_DATA_DIR = "data/raft";
    /** 默认 RPC 端口 */
    public static final int DEFAULT_RPC_PORT = 9001;
    /** 默认心跳间隔 */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 100;
    /** 默认选举超时下限 */
    public static final int DEFAULT_ELECTION_TIMEOUT_MIN_MS = 200;
    /** 默认选举超时上限 */
    public static final int DEFAULT_ELECTION_TIMEOUT_MAX_MS = 400;
    /** 默认快照触发阈值 */
    public static final int DEFAULT_SNAPSHOT_THRESHOLD = 100000;
    /** 默认日志段最大大小 32MB */
    public static final long DEFAULT_LOG_SEGMENT_SIZE_MAX = 33554432L;

    /**
     * 获取本节点在 peers 列表中的地址。
     */
    public String getSelfAddress() {
        return peers.stream()
                .filter(p -> p.id().equals(nodeId))
                .findFirst()
                .map(PeerConfig::address)
                .orElseThrow(() -> new IllegalStateException("nodeId '" + nodeId + "' not found in peers list"));
    }

    /**
     * 创建单节点默认配置（开发/测试用）。
     */
    public static RaftConfig singleNodeDefaults() {
        return new RaftConfig(
                "node1",
                DEFAULT_GROUP_ID,
                DEFAULT_DATA_DIR,
                Collections.singletonList(new PeerConfig("node1", "127.0.0.1:" + DEFAULT_RPC_PORT)),
                DEFAULT_RPC_PORT,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                DEFAULT_ELECTION_TIMEOUT_MIN_MS,
                DEFAULT_ELECTION_TIMEOUT_MAX_MS,
                DEFAULT_SNAPSHOT_THRESHOLD,
                DEFAULT_LOG_SEGMENT_SIZE_MAX
        );
    }
}
