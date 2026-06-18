package cluster;

import core.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @deprecated 已被 {@link raft.RaftServerBootstrap} + {@link raft.KvStoreStateMachine} 替代。
 *             使用 Apache Ratis 实现自动 Leader 选举和强一致性复制。
 */
@Deprecated
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
    private final AtomicLong replicationSeq;
    // 复制偏移量持久化文件
    private final File seqFile;
    // 复制模式：SYNC（同步等待ACK）或 ASYNC（异步发送不等待）
    private final ReplicationMode replicationMode;

    public enum ReplicationMode {
        /** 同步模式：等待从节点 ACK 后再返回（默认） */
        SYNC,
        /** 异步模式：发送后不等待 ACK，最高吞吐 */
        ASYNC
    }

    public MasterNode(String dataPath, List<ClusterNode> nodes) throws IOException {
        this(dataPath, nodes, ReplicationMode.SYNC);
    }

    public MasterNode(String dataPath, List<ClusterNode> nodes, ReplicationMode replicationMode) throws IOException {
        super(dataPath);
        this.clusterManager = new ClusterManager(nodes);
        this.replicationMode = replicationMode;
        this.seqFile = new File(dataPath, "replication.seq");
        this.replicationSeq = new AtomicLong(loadSeq());
        // 每秒检查一次是否需要同步
        scheduler.scheduleAtFixedRate(this::syncBatch, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * 从磁盘加载已持久化的 replication sequence。
     */
    private long loadSeq() {
        if (!seqFile.exists()) {
            return 0L;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(seqFile))) {
            long seq = in.readLong();
            log.info("加载复制序列号: {}", seq);
            return seq;
        } catch (IOException e) {
            log.warn("读取复制序列号失败，从 0 开始", e);
            return 0L;
        }
    }

    /**
     * 持久化 replication sequence 到磁盘。
     */
    private void persistSeq(long seq) {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(seqFile))) {
            out.writeLong(seq);
        } catch (IOException e) {
            log.error("持久化复制序列号失败", e);
        }
    }

    public ReplicationMode getReplicationMode() {
        return replicationMode;
    }

    public long getReplicationSeq() {
        return replicationSeq.get();
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
        return ReplicationCodec.encode(msg);
    }

    private synchronized void syncBatch() {
        if (!batch.isEmpty()) {
            // 拷贝当前批次并清空，避免集群同步期间阻塞写入
            List<byte[]> snapshot = List.copyOf(batch);
            batch.clear();
            if (replicationMode == ReplicationMode.ASYNC) {
                // 异步模式：提交到后台线程发送，不阻塞写路径
                CompletableFuture.runAsync(() -> clusterManager.syncData(snapshot));
            } else {
                // 同步模式：阻塞等待 ACK
                clusterManager.syncData(snapshot);
            }
            // 持久化当前序列号
            persistSeq(replicationSeq.get());
        }
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        syncBatch();
        persistSeq(replicationSeq.get());
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