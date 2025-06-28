package cluster;

import core.LSMStorageEngine;
import core.Constants;
import core.WALManager; // 引入 WALManager

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MasterNode extends LSMStorageEngine {
    private ClusterManager clusterManager;
    private ServerSocket serverSocket;
    private WALManager walManager; // 引入 WALManager

    // 批量同步队列
    private List<byte[]> batch = new ArrayList<>();
    // 批量大小配置
    private final int batchSize = 100;
    // 定时任务调度器
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public MasterNode(String dataPath, List<ClusterNode> nodes) throws IOException {
        super(dataPath);
        this.clusterManager = new ClusterManager(nodes);
        this.walManager = new WALManager(dataPath, 1024 * 1024); // 假设段大小为1MB
        // 每秒检查一次是否需要同步
        scheduler.scheduleAtFixedRate(this::syncBatch, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void put(byte[] key, Object value) throws IOException {
        super.put(key, value);
        byte[] data = serializeData(key, value);
        batch.add(data);
        if (batch.size() >= batchSize) {
            syncBatch();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        super.delete(key);
        byte[] data = serializeData(key, null);
        batch.add(data);
        if (batch.size() >= batchSize) {
            syncBatch();
        }
    }

    private byte[] serializeData(byte[] key, Object value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        return baos.toByteArray();
    }

    private void syncBatch() {
        if (!batch.isEmpty()) {
            clusterManager.syncData(batch);
            batch.clear();
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(12345);
        System.out.println("Master node is running on port 12345");

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from " + clientSocket.getInetAddress().getHostAddress());

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
                        case "wal_recover": // 新增 WAL 恢复请求处理
                            handleWALRecoverRequest(clientSocket);
                            break;
                        default:
                            out.println("ERROR Unknown operation");
                            break;
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void handleWALRecoverRequest(Socket socket) throws IOException {
            // 获取WAL文件路径
            String walFilePath = walManager.getWalPath();
            File walFile = new File(walFilePath);

            if (!walFile.exists()) {
                // 如果WAL文件不存在，发送空响应
                try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                    out.println("WAL file not found");
                }
                return;
            }

            // 发送WAL文件内容
            try (FileInputStream fis = new FileInputStream(walFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    socket.getOutputStream().write(buffer, 0, bytesRead);
                }
            }
        }

        private String databaseGet(String key) throws IOException, ClassNotFoundException {
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