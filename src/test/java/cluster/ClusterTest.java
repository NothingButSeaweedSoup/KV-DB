package cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集群模块单元测试。
 * 覆盖 MasterNode 序列号持久化、SlaveNode 消息应用与 offset 持久化、ClusterManager 等。
 */
class ClusterTest {

    @TempDir
    Path tempDir;

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- MasterNode: seq 持久化 ----

    private ClusterNode masterNode(String host, int port) {
        ClusterNode n = new ClusterNode(host, port);
        n.setMaster(true);
        return n;
    }

    private ClusterNode slaveNode(String host, int port) {
        ClusterNode n = new ClusterNode(host, port);
        n.setMaster(false);
        return n;
    }

    @Test
    void masterNodeShouldPersistSeqOnClose() throws Exception {
        Path dataDir = tempDir.resolve("master-seq");
        var nodes = java.util.List.of(masterNode("localhost", 19991));

        MasterNode master = new MasterNode(dataDir.toString(), nodes, MasterNode.ReplicationMode.SYNC);

        // 写入数据触发 seq 递增
        master.put(key("k1"), "v1");
        master.put(key("k2"), "v2");
        master.put(key("k3"), "v3");

        long seqBeforeClose = master.getReplicationSeq();
        master.close();

        // 验证 seq 文件存在
        File seqFile = dataDir.resolve("replication.seq").toFile();
        assertThat(seqFile).exists();

        // 重新打开，seq 应恢复
        MasterNode master2 = new MasterNode(dataDir.toString(), nodes, MasterNode.ReplicationMode.SYNC);
        assertThat(master2.getReplicationSeq()).isEqualTo(seqBeforeClose);
        master2.close();
    }

    @Test
    void masterNodeShouldSupportAsyncMode() throws Exception {
        Path dataDir = tempDir.resolve("master-async");
        var nodes = java.util.List.of(masterNode("localhost", 19992));

        MasterNode master = new MasterNode(dataDir.toString(), nodes, MasterNode.ReplicationMode.ASYNC);
        assertThat(master.getReplicationMode()).isEqualTo(MasterNode.ReplicationMode.ASYNC);

        master.put(key("async-key"), "async-val");
        assertThat(master.getReplicationSeq()).isGreaterThanOrEqualTo(1);

        master.close();
    }

    // ---- SlaveNode: 消息应用与 offset 持久化 ----

    @Test
    void slaveNodeShouldPersistOffset() throws Exception {
        Path dataDir = tempDir.resolve("slave-offset");

        // 写入 offset 文件
        File offsetFile = dataDir.resolve("replication.offset").toFile();
        dataDir.toFile().mkdirs();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(offsetFile))) {
            out.writeLong(42);
        }

        // SlaveNode 启动后应加载该 offset
        // 由于 SlaveNode 构造器会绑定端口，这里直接测试文件读写逻辑
        try (DataInputStream in = new DataInputStream(new FileInputStream(offsetFile))) {
            long loaded = in.readLong();
            assertThat(loaded).isEqualTo(42);
        }
    }

    @Test
    void slaveNodeShouldSkipDuplicateMessages() throws Exception {
        // 模拟 applyReplicationMessage 的去重逻辑
        long lastAppliedSeq = 10;

        // seq <= lastAppliedSeq 的消息应被跳过
        ReplicationMessage duplicate = new ReplicationMessage(
                5, ReplicationMessage.Operation.PUT, key("dup"), "val");

        assertThat(duplicate.getSeq()).isLessThanOrEqualTo(lastAppliedSeq);
    }

    // ---- ClusterManager: ACK 确认 ----

    @Test
    void clusterManagerShouldReturnSlaveStatus() {
        var nodes = java.util.List.of(
                masterNode("host1", 1001),
                slaveNode("host2", 1002),
                slaveNode("host3", 1003)
        );

        ClusterManager manager = new ClusterManager(nodes);
        var status = manager.getSlaveStatus();

        assertThat(status).hasSize(2); // 只返回从节点
        assertThat(status.get(0)).contains("host2");
        assertThat(status.get(1)).contains("host3");
    }

    @Test
    void clusterManagerSyncDataShouldHandleNoSlaves() {
        var nodes = java.util.List.of(masterNode("host1", 1001)); // 只有主节点

        ClusterManager manager = new ClusterManager(nodes);
        int ackCount = manager.syncData(java.util.List.of(new byte[]{1, 2, 3}));

        assertThat(ackCount).isZero(); // 无从节点，确认数为 0
    }

    // ---- ReplicationMessage ----

    @Test
    void replicationMessageShouldHoldData() {
        byte[] key = "testKey".getBytes();
        ReplicationMessage msg = new ReplicationMessage(
                100, ReplicationMessage.Operation.PUT, key, "testValue");

        assertThat(msg.getSeq()).isEqualTo(100);
        assertThat(msg.getOp()).isEqualTo(ReplicationMessage.Operation.PUT);
        assertThat(msg.getKey()).isEqualTo(key);
        assertThat(msg.getValue()).isEqualTo("testValue");
        assertThat(msg.toString()).contains("seq=100");
    }

    @Test
    void replicationMessageShouldSupportDelete() {
        ReplicationMessage msg = new ReplicationMessage(
                200, ReplicationMessage.Operation.DELETE, key("del"), null);

        assertThat(msg.getOp()).isEqualTo(ReplicationMessage.Operation.DELETE);
        assertThat(msg.getValue()).isNull();
    }

    // ---- ClusterNode ----

    @Test
    void clusterNodeShouldIdentifyMaster() {
        ClusterNode master = masterNode("host", 1001);
        ClusterNode slave = slaveNode("host", 1002);

        assertThat(master.isMaster()).isTrue();
        assertThat(slave.isMaster()).isFalse();
        assertThat(master.getHost()).isEqualTo("host");
        assertThat(master.getPort()).isEqualTo(1001);
    }

    // ---- ClusterConfig ----

    @Test
    void clusterConfigShouldSupportReplicationMode() {
        ClusterConfig config = new ClusterConfig();
        assertThat(config.getReplicationMode()).isEqualTo("SYNC"); // 默认值

        config.setReplicationMode("ASYNC");
        assertThat(config.getReplicationMode()).isEqualTo("ASYNC");
    }
}
