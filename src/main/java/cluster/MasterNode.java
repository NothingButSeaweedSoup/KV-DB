package cluster;

import core.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MasterNode extends LSMStorageEngine {

    private static final Logger log = LoggerFactory.getLogger(MasterNode.class);

    private final ClusterManager clusterManager;
    private ServerSocket serverSocket;

    // 批量同步队列（线程安全）
    private final List<byte[]> batch = new CopyOnWriteArrayList<>();
    // 批量大小配置
    private final int batchSize = 100;
    // 定时任务调度器
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicLong replicationSeq = new AtomicLong(0);

    public MasterNode(String dataPath, List<ClusterNode> nodes) throws IOException {
        super(dataPath);
        this.clusterManager = new ClusterManager(nodes);
        // 每秒检查一次是否需要同步
        scheduler.scheduleAtFixedRate(this::syncBatch, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void put(byte[] key, Object value) throws IOException {
        super.put(key, value);
        byte[] data = serializeData(ReplicationMessage.Operation.PUT, key, value);
        batch.add(data);
        if (batch.size() >= batchSize) {
            syncBatch();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        super.delete(key);
        byte[] data = serializeData(ReplicationMessage.Operation.DELETE, key, null);
        batch.add(data);
        if (batch.size() >= batchSize) {
            syncBatch();
        }
    }

    private byte[] serializeData(ReplicationMessage.Operation op, byte[] key, Object value) throws IOException {
        long seq = replicationSeq.incrementAndGet();
        ReplicationMessage msg = new ReplicationMessage(seq, op, key, value);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(msg);
        }
        return baos.toByteArray();
    }

    private synchronized void syncBatch() {
        if (!batch.isEmpty()) {
            // 拷贝当前批次并清空，避免集群同步期间阻塞写入
            List<byte[]> snapshot = List.copyOf(batch);
            batch.clear();
            clusterManager.syncData(snapshot);
        }
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        syncBatch();
        super.close();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(12345);
        log.info("Master node is running on port 12345");

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                log.info("Accepted connection from {}", clientSocket.getInetAddress().getHostAddress());

                // 处理客户端请求
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } finally {
            serverSocket.close();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    // 解析客户端请求
                    String[] parts = inputLine.split(" ", 3);
                    String operation = parts[0].toLowerCase();
                    String key = parts.length > 1 ? parts[1] : null;
                    String value = parts.length > 2 ? parts[2] : null;

                    // 执行数据库操作
                    switch (operation) {
                        case "get":
                            String result = "Usage: get <key>";
                            if (key != null) {
                                result = databaseGet(key);
                            }
                            out.println(result);
                            break;
                        case "put":
                            if (key != null && value != null) {
                                databasePut(key, value);
                                out.println("OK");
                            } else {
                                out.println("Usage: put <key> <value>");
                            }
                            break;
                        case "delete":
                            if (key != null) {
                                databaseDelete(key);
                                out.println("OK");
                            } else {
                                out.println("Usage: delete <key>");
                            }
                            break;
                        case "flush":
                            databaseFlush();
                            out.println("OK");
                            break;
                        case "exit":
                            out.println("Server is shutting down.");
                            break;
                        default:
                            out.println("ERROR Unknown operation");
                            break;
                    }
                }
            } catch (IOException e) {
                log.error("处理客户端请求异常", e);
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    log.warn("关闭客户端连接失败", e);
                }
            }
        }

        private String databaseGet(String key) throws IOException {
            Object result = get(key.getBytes());
            return result != null ? result.toString() : "null";
        }


        private void databasePut(String key, String value) throws IOException {
            put(key.getBytes(), value);
        }

        private void databaseDelete(String key) throws IOException {
            delete(key.getBytes());
        }

        private void databaseFlush() throws IOException {
            flush();
        }
    }
}