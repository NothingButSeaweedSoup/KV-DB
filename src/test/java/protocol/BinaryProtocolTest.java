package protocol;

import client.BinaryClient;
import core.LSMStorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import server.NioServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 二进制协议端到端测试。
 * 启动 NIO 服务器，通过 BinaryClient 验证完整的请求-响应流程。
 */
class BinaryProtocolTest {

    @TempDir
    Path tempDir;

    private static final int PORT = 19876;
    private LSMStorageEngine engine;
    private NioServer server;
    private ExecutorService serverExecutor;

    @BeforeEach
    void setUp() throws Exception {
        engine = new LSMStorageEngine(tempDir.resolve("data").toString());
        server = new NioServer(PORT, engine);
        serverExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "test-nio-server");
            t.setDaemon(true);
            return t;
        });
        serverExecutor.submit(() -> {
            try {
                server.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // 等待服务器端口就绪
        boolean ready = false;
        for (int i = 0; i < 30; i++) {
            try (Socket probe = new Socket("localhost", PORT)) {
                ready = true;
                break;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        if (!ready) {
            throw new RuntimeException("服务器未在 3 秒内就绪");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
        engine.close();
        serverExecutor.shutdownNow();
        serverExecutor.awaitTermination(3, TimeUnit.SECONDS);
    }

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] val(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void shouldPing() throws Exception {
        try (BinaryClient client = new BinaryClient("localhost", PORT)) {
            assertThat(client.ping()).isTrue();
        }
    }

    @Test
    void shouldPutAndGet() throws Exception {
        try (BinaryClient client = new BinaryClient("localhost", PORT)) {
            client.put(key("hello"), val("world"));

            byte[] result = client.get(key("hello"));
            assertThat(result).isNotNull();
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("world");
        }
    }

    @Test
    void shouldReturnNullForMissingKey() throws Exception {
        try (BinaryClient client = new BinaryClient("localhost", PORT)) {
            byte[] result = client.get(key("nonexistent"));
            assertThat(result).isNull();
        }
    }

    @Test
    void shouldDeleteKey() throws Exception {
        try (BinaryClient client = new BinaryClient("localhost", PORT)) {
            client.put(key("toDelete"), val("value"));
            assertThat(client.get(key("toDelete"))).isNotNull();

            client.delete(key("toDelete"));
            assertThat(client.get(key("toDelete"))).isNull();
        }
    }

    @Test
    void shouldGetStats() throws Exception {
        try (BinaryClient client = new BinaryClient("localhost", PORT)) {
            client.put(key("s1"), val("v1"));
            client.put(key("s2"), val("v2"));
            client.get(key("s1"));

            String stats = client.stats();
            assertThat(stats).contains("put_total=");
            assertThat(stats).contains("get_total=");
        }
    }

    @Test
    void shouldHandleMultipleConsecutiveOperations() throws Exception {
        try (BinaryClient client = new BinaryClient("localhost", PORT)) {
            for (int i = 0; i < 50; i++) {
                client.put(key("multi-" + i), val("value-" + i));
            }

            for (int i = 0; i < 50; i++) {
                byte[] result = client.get(key("multi-" + i));
                assertThat(result).isNotNull();
                assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("value-" + i);
            }
        }
    }

    @Test
    void shouldHandleReconnection() throws Exception {
        BinaryClient client = new BinaryClient("localhost", PORT);
        client.put(key("reconn"), val("v1"));
        client.disconnect();

        // 重新连接
        client.put(key("reconn"), val("v2"));
        byte[] result = client.get(key("reconn"));
        assertThat(result).isNotNull();
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("v2");
        client.close();
    }

    @Test
    void shouldHandleConcurrentClients() throws Exception {
        int clientCount = 5;
        int opsPerClient = 20;

        CompletableFuture<?>[] futures = new CompletableFuture[clientCount];
        for (int c = 0; c < clientCount; c++) {
            final int clientId = c;
            futures[c] = CompletableFuture.runAsync(() -> {
                try (BinaryClient client = new BinaryClient("localhost", PORT, 10000)) {
                    for (int i = 0; i < opsPerClient; i++) {
                        byte[] k = key("client" + clientId + "-key" + i);
                        byte[] v = val("value" + i);
                        client.put(k, v);
                        byte[] result = client.get(k);
                        assertThat(result).isNotNull();
                        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("value" + i);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);
    }
}
