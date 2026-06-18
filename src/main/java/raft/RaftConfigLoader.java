package raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 从 raft-config.json 加载 Raft 配置。文件不存在时返回单节点默认配置。
 */
public class RaftConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(RaftConfigLoader.class);
    private static final String DEFAULT_CONFIG_FILE = "raft-config.json";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 加载默认路径的配置文件。
     */
    public static RaftConfig load() {
        return load(DEFAULT_CONFIG_FILE);
    }

    /**
     * 从指定路径加载配置文件。文件不存在时返回单节点默认配置。
     */
    public static RaftConfig load(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            log.warn("配置文件 {} 不存在，使用单节点默认配置", filePath);
            return RaftConfig.singleNodeDefaults();
        }

        try {
            JsonNode root = MAPPER.readTree(file);

            String nodeId = getText(root, "nodeId", "node1");
            String raftGroupId = getText(root, "raftGroupId", RaftConfig.DEFAULT_GROUP_ID);
            String dataDir = getText(root, "dataDir", RaftConfig.DEFAULT_DATA_DIR);

            // 解析 peers
            List<RaftConfig.PeerConfig> peers = new ArrayList<>();
            JsonNode peersNode = root.get("peers");
            if (peersNode != null && peersNode.isArray()) {
                for (JsonNode peerNode : peersNode) {
                    String id = peerNode.get("id").asText();
                    String address = peerNode.get("address").asText();
                    peers.add(new RaftConfig.PeerConfig(id, address));
                }
            }
            if (peers.isEmpty()) {
                peers.add(new RaftConfig.PeerConfig("node1", "127.0.0.1:" + RaftConfig.DEFAULT_RPC_PORT));
            }

            // 解析 raft 子对象
            JsonNode raftNode = root.get("raft");
            int rpcPort = getInt(raftNode, "rpcPort", RaftConfig.DEFAULT_RPC_PORT);
            int heartbeatIntervalMs = getInt(raftNode, "heartbeatIntervalMs", RaftConfig.DEFAULT_HEARTBEAT_INTERVAL_MS);
            int electionTimeoutMinMs = getInt(raftNode, "electionTimeoutMinMs", RaftConfig.DEFAULT_ELECTION_TIMEOUT_MIN_MS);
            int electionTimeoutMaxMs = getInt(raftNode, "electionTimeoutMaxMs", RaftConfig.DEFAULT_ELECTION_TIMEOUT_MAX_MS);
            int snapshotThreshold = getInt(raftNode, "snapshotAutoTriggerThreshold", RaftConfig.DEFAULT_SNAPSHOT_THRESHOLD);
            long logSegmentSizeMax = getLong(raftNode, "logSegmentSizeMax", RaftConfig.DEFAULT_LOG_SEGMENT_SIZE_MAX);

            RaftConfig config = new RaftConfig(
                    nodeId, raftGroupId, dataDir, peers,
                    rpcPort, heartbeatIntervalMs,
                    electionTimeoutMinMs, electionTimeoutMaxMs,
                    snapshotThreshold, logSegmentSizeMax
            );

            log.info("Raft 配置加载成功: nodeId={}, address={}, dataDir={}",
                    config.nodeId(), config.getSelfAddress(), config.dataDir());
            return config;

        } catch (IOException e) {
            log.warn("读取配置文件 {} 失败: {}，使用单节点默认配置", filePath, e.getMessage());
            return RaftConfig.singleNodeDefaults();
        }
    }

    private static String getText(JsonNode node, String field, String defaultValue) {
        if (node == null) return defaultValue;
        JsonNode child = node.get(field);
        return child != null ? child.asText() : defaultValue;
    }

    private static int getInt(JsonNode node, String field, int defaultValue) {
        if (node == null) return defaultValue;
        JsonNode child = node.get(field);
        return child != null ? child.asInt() : defaultValue;
    }

    private static long getLong(JsonNode node, String field, long defaultValue) {
        if (node == null) return defaultValue;
        JsonNode child = node.get(field);
        return child != null ? child.asLong() : defaultValue;
    }
}
