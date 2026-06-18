package raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 三节点 Raft 集成测试。
 * <p>
 * 测试场景：
 * - 三节点正常写读
 * - Leader 宕机后恢复写入
 * - 节点重启后数据恢复
 * - 快照触发与恢复
 */
class RaftClusterTest {

    private static final Logger log = LoggerFactory.getLogger(RaftClusterTest.class);

    @TempDir
    Path tempDir;

    private RaftClusterTestHelper cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new RaftClusterTestHelper(3, tempDir);
        cluster.startAll();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    // ======================== 三节点正常写读 ========================

    @Test
    void threeNode_putAndGet() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 写入多个键值对
        client.put("key1".getBytes(), "value1");
        client.put("key2".getBytes(), 42);
        client.put("key3".getBytes(), true);

        // 读取验证
        assertThat(client.get("key1".getBytes())).isEqualTo("value1");
        assertThat(client.get("key2".getBytes())).isEqualTo(42);
        assertThat(client.get("key3".getBytes())).isEqualTo(true);

        log.info("三节点正常写读测试通过");
    }

    @Test
    void threeNode_putDelete() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 写入
        client.put("toDelete".getBytes(), "value");
        assertThat(client.get("toDelete".getBytes())).isEqualTo("value");

        // 删除
        client.delete("toDelete".getBytes());
        assertThat(client.get("toDelete".getBytes())).isNull();

        log.info("三节点删除测试通过");
    }

    // ======================== Leader 宕机后恢复写入 ========================

    @Test
    void threeNode_leaderFailover() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 1. 确认集群正常：写入数据
        client.put("before-failover".getBytes(), "v1");
        assertThat(client.get("before-failover".getBytes())).isEqualTo("v1");

        String leaderBefore = cluster.getLeaderId();
        log.info("当前 Leader: {}", leaderBefore);
        assertThat(leaderBefore).isNotNull();

        // 2. 关闭 Leader
        int killedIndex = cluster.killLeader();
        log.info("已关闭 Leader: {} (index={})", leaderBefore, killedIndex);
        assertThat(cluster.getAliveNodeCount()).isEqualTo(2);

        // 3. 等待新 Leader 选出
        cluster.waitForNewLeader();

        String leaderAfter = cluster.getLeaderId();
        log.info("新 Leader: {}", leaderAfter);
        assertThat(leaderAfter).isNotNull();
        assertThat(leaderAfter).isNotEqualTo(leaderBefore);

        // 4. 在新 Leader 下写入数据
        client.put("after-failover".getBytes(), "v2");

        // 5. 验证数据
        assertThat(client.get("before-failover".getBytes())).isEqualTo("v1");
        assertThat(client.get("after-failover".getBytes())).isEqualTo("v2");

        log.info("Leader 故障切换测试通过: {} -> {}", leaderBefore, leaderAfter);
    }

    // ======================== 节点重启后数据恢复 ========================

    @Test
    void threeNode_nodeRestart() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 1. 写入初始数据
        client.put("before-restart".getBytes(), "v1");
        assertThat(client.get("before-restart".getBytes())).isEqualTo("v1");

        // 2. 找到一个 Follower 并关闭
        String leaderId = cluster.getLeaderId();
        int followerIndex = -1;
        for (int i = 0; i < 3; i++) {
            if (!cluster.nodeIds.get(i).equals(leaderId)) {
                followerIndex = i;
                break;
            }
        }
        assertThat(followerIndex).isGreaterThanOrEqualTo(0);
        log.info("关闭 Follower: {} (index={})", cluster.nodeIds.get(followerIndex), followerIndex);
        cluster.stopNode(followerIndex);

        // 3. 在 Follower 关闭期间继续写入
        client.put("during-offline".getBytes(), "v2");
        assertThat(client.get("during-offline".getBytes())).isEqualTo("v2");

        // 4. 重启 Follower
        cluster.restartNode(followerIndex);
        log.info("Follower {} 已重启", cluster.nodeIds.get(followerIndex));

        // 5. 等待日志同步
        Thread.sleep(3000);

        // 6. 验证所有数据仍然可读（通过 Leader 读取）
        assertThat(client.get("before-restart".getBytes())).isEqualTo("v1");
        assertThat(client.get("during-offline".getBytes())).isEqualTo("v2");

        log.info("节点重启恢复测试通过");
    }

    // ======================== 快照触发与恢复 ========================

    @Test
    void threeNode_snapshotAndRecovery() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 1. 写入超过快照阈值的数据（阈值配置为 10 条）
        int entryCount = 15;
        for (int i = 0; i < entryCount; i++) {
            client.put(("snapshot-key-" + i).getBytes(), ("snapshot-value-" + i));
        }
        log.info("已写入 {} 条数据", entryCount);

        // 2. 等待快照触发
        Thread.sleep(2000);

        // 3. 验证数据正确
        for (int i = 0; i < entryCount; i++) {
            Object value = client.get(("snapshot-key-" + i).getBytes());
            assertThat(value).isEqualTo("snapshot-value-" + i);
        }

        // 4. 关闭并重启一个节点，验证从快照恢复
        String leaderId = cluster.getLeaderId();
        int followerIndex = -1;
        for (int i = 0; i < 3; i++) {
            if (!cluster.nodeIds.get(i).equals(leaderId)) {
                followerIndex = i;
                break;
            }
        }
        cluster.restartNode(followerIndex);
        Thread.sleep(3000);

        // 5. 验证数据在重启后仍然完整
        for (int i = 0; i < entryCount; i++) {
            Object value = client.get(("snapshot-key-" + i).getBytes());
            assertThat(value).isEqualTo("snapshot-value-" + i);
        }

        log.info("快照触发与恢复测试通过");
    }

    // ======================== 多次故障切换 ========================

    @Test
    void threeNode_multipleFailovers() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 写入初始数据
        client.put("stable-key".getBytes(), "stable-value");

        // 进行两次 Leader 切换
        for (int round = 1; round <= 2; round++) {
            String leaderBefore = cluster.getLeaderId();
            log.info("第 {} 轮故障切换, 当前 Leader: {}", round, leaderBefore);

            // 关闭 Leader
            cluster.killLeader();

            // 等待新 Leader
            cluster.waitForNewLeader();

            String leaderAfter = cluster.getLeaderId();
            log.info("第 {} 轮: Leader {} -> {}", round, leaderBefore, leaderAfter);
            assertThat(leaderAfter).isNotNull();
            assertThat(leaderAfter).isNotEqualTo(leaderBefore);

            // 在新 Leader 下写入数据
            client.put(("round-" + round).getBytes(), ("value-" + round));

            // 重启旧 Leader（恢复为 Follower）
            int oldLeaderIndex = cluster.nodeIds.indexOf(leaderBefore);
            cluster.restartNode(oldLeaderIndex);
            Thread.sleep(2000);
        }

        // 验证所有数据完整
        assertThat(client.get("stable-key".getBytes())).isEqualTo("stable-value");
        assertThat(client.get("round-1".getBytes())).isEqualTo("value-1");
        assertThat(client.get("round-2".getBytes())).isEqualTo("value-2");

        log.info("多次故障切换测试通过");
    }
}
