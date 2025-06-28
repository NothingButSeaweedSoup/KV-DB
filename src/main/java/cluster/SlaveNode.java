package cluster;

import core.LSMStorageEngine;
import core.Constants;
import core.WALManager; // 引入 WALManager

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class SlaveNode extends LSMStorageEngine {
    private ServerSocket serverSocket;
    private ClusterConfig config; // 用于获取主节点信息

    public SlaveNode(String dataPath, int port, ClusterConfig config) throws IOException {
        super(dataPath);
        this.serverSocket = new ServerSocket(port);
        this.config = config; // 保存配置信息
    }

    public void start() throws IOException {
        // 启动一个新线程用于处理 WAL 恢复
        new Thread(this::recoverFromWAL).start();

        try {
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new ClientHandler(socket)).start();
            }
        } finally {
            serverSocket.close();
        }
    }

    // 在 SlaveNode 中新增 recoverFromWAL 方法
    private void recoverFromWAL() {
        while (true) {
            try {
                // 假设主节点的WAL文件路径是已知的
                String walFilePath = getMasterWALFilePath();
                List<WALManager.WALEntry> walEntries = fetchWALEntries(walFilePath);
                replayWALEntries(walEntries); // 重放WAL日志
                Thread.sleep(1000); // 每秒拉取一次
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String getMasterWALFilePath() {
        // 从配置文件中获取主节点的WAL文件路径
        return config.getDataPath() + Constants.File.WAL_EXTENSION;
    }

    // 在 SlaveNode 中新增 fetchWALEntries 方法
    private List<WALManager.WALEntry> fetchWALEntries(String walFilePath) throws IOException {
        List<WALManager.WALEntry> entries = new ArrayList<>();
        File walFile = new File(walFilePath);
        if (!walFile.exists()) {
            return entries; // 如果WAL文件不存在，返回空列表
        }

        try (FileInputStream fis = new FileInputStream(walFile);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            while (true) {
                try {
                    WALManager.WALEntry entry = (WALManager.WALEntry) ois.readObject();
                    entries.add(entry);
                } catch (ClassNotFoundException | EOFException e) {
                    break; // 结束读取
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entries;
    }

    // 在 SlaveNode 中新增 replayWALEntries 方法
    private void replayWALEntries(List<WALManager.WALEntry> entries) throws IOException {
        for (WALManager.WALEntry entry : entries) {
            switch (entry.getOperation()) {
                case Constants.Operation.PUT:
                    put(entry.getKey(), entry.getValue());
                    break;
                case Constants.Operation.DELETE:
                    delete(entry.getKey());
                    break;
            }
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (InputStream in = clientSocket.getInputStream()) {
                // 读取数据列表的大小
                int dataSize = bytesToInt(readBytes(in, 4));
                for (int i = 0; i < dataSize; i++) {
                    // 读取每个数据的长度
                    int dataLength = bytesToInt(readBytes(in, 4));
                    // 读取数据本身
                    byte[] data = readBytes(in, dataLength);
                    // 处理接收到的数据
                    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                        Object value = ois.readObject();
                        // 更新本地存储
                        put("received_key".getBytes(), value);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 辅助方法：从InputStream中读取指定长度的字节
        private byte[] readBytes(InputStream in, int length) throws IOException {
            byte[] buffer = new byte[length];
            int bytesRead = 0;
            while (bytesRead < length) {
                int result = in.read(buffer, bytesRead, length - bytesRead);
                if (result == -1) {
                    throw new IOException("Unexpected end of stream");
                }
                bytesRead += result;
            }
            return buffer;
        }

        // 辅助方法：将4字节的byte数组转换为int
        private int bytesToInt(byte[] bytes) {
            return (bytes[0] << 24) |
                    (bytes[1] << 16) |
                    (bytes[2] << 8) |
                    (bytes[3] & 0xFF);
        }
    }
}