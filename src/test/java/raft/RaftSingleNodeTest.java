package raft;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 单节点 Ratis 集成测试：启动 Server，通过 Client 执行 put/get/delete。
 */
class RaftSingleNodeTest {

    @TempDir
    Path tempDir;

    private RaftServerBootstrap server;
    private RaftKVClient client;
    private int port;
    private String groupId;

    @BeforeEach
    void setUp() throws Exception {
        // 找一个可用端口
        port = findAvailablePort();
        groupId = UUID.randomUUID().toString();

        // 构建配置
        String nodeId = "test-node";
        String address = "127.0.0.1:" + port;
        RaftConfig.PeerConfig peer = new RaftConfig.PeerConfig(nodeId, address);
        RaftConfig config = new RaftConfig(
                nodeId,
                groupId,
                tempDir.toString(),
                Collections.singletonList(peer),
                port,
                100,   // heartbeat
                200,   // election min
                400,   // election max
                1000,  // snapshot threshold (低一些，加速测试)
                33554432L
        );

        // 启动服务端
        server = new RaftServerBootstrap(config);
        server.start();

        // 等待 Leader 选举完成（单节点会立即成为 Leader）
        Thread.sleep(1000);

        // 创建客户端
        client = new RaftKVClient(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void putAndGet() throws Exception {
        byte[] key = "hello".getBytes();
        String value = "world";

        // put
        client.put(key, value);

        // get
        Object result = client.get(key);
        assertThat(result).isEqualTo("world");
    }

    @Test
    void putAndGet_multipleEntries() throws Exception {
        // 写入多个键值对
        client.put("key1".getBytes(), "value1");
        client.put("key2".getBytes(), 42);
        client.put("key3".getBytes(), true);

        // 读取并验证
        assertThat(client.get("key1".getBytes())).isEqualTo("value1");
        assertThat(client.get("key2".getBytes())).isEqualTo(42);
        assertThat(client.get("key3".getBytes())).isEqualTo(true);
    }

    @Test
    void delete() throws Exception {
        byte[] key = "toDelete".getBytes();

        // 先写入
        client.put(key, "value");
        assertThat(client.get(key)).isEqualTo("value");

        // 删除
        client.delete(key);

        // 验证已删除
        assertThat(client.get(key)).isNull();
    }

    @Test
    void get_nonExistentKey_returnsNull() throws Exception {
        Object result = client.get("nonExistent".getBytes());
        assertThat(result).isNull();
    }

    @Test
    void putOverwrite() throws Exception {
        byte[] key = "overwrite".getBytes();

        client.put(key, "first");
        assertThat(client.get(key)).isEqualTo("first");

        client.put(key, "second");
        assertThat(client.get(key)).isEqualTo("second");
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
