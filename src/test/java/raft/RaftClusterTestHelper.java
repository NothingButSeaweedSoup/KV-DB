package raft;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 三节点 Raft 集群测试辅助类。
 * <p>
 * 管理多个 RaftServerBootstrap 实例的生命周期，提供故障注入方法。
 */
public class RaftClusterTestHelper {

    private static final Logger log = LoggerFactory.getLogger(RaftClusterTestHelper.class);

    private final int nodeCount;
    private final Path baseDir;
    private final String groupId;
    private final List<Integer> ports;
    private final List<RaftServerBootstrap> servers;
    final List<String> nodeIds;
    private final List<String> addresses;
    private RaftKVClient client;

    /**
     * 创建集群辅助类。
     *
     * @param nodeCount 节点数量（通常为 3）
     * @param baseDir   基础数据目录（每个节点在子目录下）
     */
    public RaftClusterTestHelper(int nodeCount, Path baseDir) throws IOException {
        this.nodeCount = nodeCount;
        this.baseDir = baseDir;
        this.groupId = UUID.randomUUID().toString();
        this.ports = new ArrayList<>();
        this.servers = new ArrayList<>();
        this.nodeIds = new ArrayList<>();
        this.addresses = new ArrayList<>();

        // 为每个节点分配端口和 ID
        for (int i = 0; i < nodeCount; i++) {
            ports.add(findAvailablePort());
            nodeIds.add("node-" + i);
            addresses.add("127.0.0.1:" + ports.get(i));
        }
    }

    /**
     * 启动所有节点。
     */
    public void startAll() throws Exception {
        log.info("启动 {} 节点集群, groupId={}", nodeCount, groupId);

        for (int i = 0; i < nodeCount; i++) {
            startNode(i);
        }

        // 等待 Leader 选举完成
        waitForLeaderElection();

        // 创建客户端
        client = createClient();

        log.info("集群启动完成, Leader={}", getLeaderId());
    }

    /**
     * 启动指定节点。
     */
    public void startNode(int index) throws Exception {
        if (index < 0 || index >= nodeCount) {
            throw new IllegalArgumentException("Invalid node index: " + index);
        }

        // 如果节点已启动，先关闭
        if (servers.size() > index && servers.get(index) != null) {
            stopNode(index);
        }

        // 构建 peers 列表（所有节点）
        List<RaftConfig.PeerConfig> peers = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            peers.add(new RaftConfig.PeerConfig(nodeIds.get(i), addresses.get(i)));
        }

        // 为该节点使用较低的选举超时（加速测试）
        RaftConfig config = new RaftConfig(
                nodeIds.get(index),
                groupId,
                baseDir.resolve(nodeIds.get(index)).toString(),
                peers,
                ports.get(index),
                100,   // heartbeatIntervalMs
                300,   // electionTimeoutMinMs
                600,   // electionTimeoutMaxMs
                10,    // snapshotThreshold（低阈值，加速快照测试）
                33554432L
        );

        RaftServerBootstrap server = new RaftServerBootstrap(config);
        server.start();

        // 确保 servers 列表足够大
        while (servers.size() <= index) {
            servers.add(null);
        }
        servers.set(index, server);

        log.info("节点 {} 启动成功: {}", nodeIds.get(index), addresses.get(index));
    }

    /**
     * 停止指定节点。
     */
    public void stopNode(int index) throws IOException {
        if (index < 0 || index >= servers.size()) return;
        RaftServerBootstrap server = servers.get(index);
        if (server != null) {
            log.info("关闭节点 {}: {}", nodeIds.get(index), addresses.get(index));
            server.close();
            servers.set(index, null);
        }
    }

    /**
     * 关闭 Leader 节点。
     *
     * @return 被关闭的 Leader 节点索引
     */
    public int killLeader() throws IOException {
        String leaderId = getLeaderId();
        int leaderIndex = nodeIds.indexOf(leaderId);
        if (leaderIndex < 0) {
            throw new IllegalStateException("Cannot find leader: " + leaderId);
        }
        log.info("关闭 Leader: {} (index={})", leaderId, leaderIndex);
        stopNode(leaderIndex);
        return leaderIndex;
    }

    /**
     * 重启指定节点。
     */
    public void restartNode(int index) throws Exception {
        log.info("重启节点 {}", nodeIds.get(index));
        stopNode(index);
        Thread.sleep(500); // 等待端口释放
        startNode(index);
    }

    /**
     * 等待 Leader 选举完成。
     */
    public void waitForLeaderElection() throws InterruptedException {
        waitForLeader(5000);
    }

    /**
     * 等待新 Leader 选出（在 Leader 宕机后调用）。
     */
    public void waitForNewLeader() throws InterruptedException {
        waitForLeader(5000);
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeaderId() != null) {
                return;
            }
            Thread.sleep(100);
        }
    }

    /**
     * 获取当前 Leader 的 ID。
     * 通过查询每个存活节点的 DivisionInfo 来确定。
     */
    public String getLeaderId() {
        for (int i = 0; i < nodeCount; i++) {
            if (servers.get(i) == null) continue;
            try {
                RaftServer server = servers.get(i).getServer();
                for (var groupId : server.getGroupIds()) {
                    var division = server.getDivision(groupId);
                    if (division.getInfo().getCurrentRole()
                            == org.apache.ratis.proto.RaftProtos.RaftPeerRole.LEADER) {
                        return nodeIds.get(i);
                    }
                }
            } catch (Exception e) {
                // 节点可能已关闭
            }
        }
        return null;
    }

    /**
     * 获取存活节点数量。
     */
    public int getAliveNodeCount() {
        int count = 0;
        for (RaftServerBootstrap server : servers) {
            if (server != null) count++;
        }
        return count;
    }

    /**
     * 创建连接到集群的客户端。
     */
    public RaftKVClient createClient() {
        List<RaftConfig.PeerConfig> peers = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            peers.add(new RaftConfig.PeerConfig(nodeIds.get(i), addresses.get(i)));
        }

        RaftConfig config = new RaftConfig(
                "client",
                groupId,
                baseDir.resolve("client").toString(),
                peers,
                0, 0, 0, 0, 0, 0
        );

        return new RaftKVClient(config, 10, 500);
    }

    /**
     * 获取客户端（集群启动后可用）。
     */
    public RaftKVClient getClient() {
        return client;
    }

    /**
     * 关闭客户端和所有节点。
     */
    public void shutdown() {
        log.info("关闭集群...");
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                log.warn("关闭客户端失败", e);
            }
            client = null;
        }
        for (int i = 0; i < servers.size(); i++) {
            try {
                stopNode(i);
            } catch (IOException e) {
                log.warn("关闭节点 {} 失败", i, e);
            }
        }
        servers.clear();
    }

    /**
     * 找一个可用的本地端口。
     */
    private static int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
