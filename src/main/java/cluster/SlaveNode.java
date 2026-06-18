package cluster;

import core.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;

public class SlaveNode extends LSMStorageEngine {

    private static final Logger log = LoggerFactory.getLogger(SlaveNode.class);

    private final ServerSocket serverSocket;
    private final File replicationOffsetFile;
    private volatile long lastAppliedSeq;

    public SlaveNode(String dataPath, int port, ClusterConfig config) throws IOException {
        super(dataPath);
        this.serverSocket = new ServerSocket(port);
        this.replicationOffsetFile = new File(dataPath, "replication.offset");
        this.lastAppliedSeq = loadLastAppliedSeq();
    }

    public void start() throws IOException {
        log.info("从节点启动，监听端口 {}", serverSocket.getLocalPort());
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                log.info("接受来自 {} 的连接", socket.getInetAddress().getHostAddress());
                new Thread(new ClientHandler(socket)).start();
            }
        } finally {
            serverSocket.close();
        }
    }

    private long loadLastAppliedSeq() {
        if (!replicationOffsetFile.exists()) {
            return 0L;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(replicationOffsetFile))) {
            return in.readLong();
        } catch (IOException e) {
            log.error("读取复制偏移量失败", e);
            return 0L;
        }
    }

    private void persistLastAppliedSeq(long seq) {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(replicationOffsetFile))) {
            out.writeLong(seq);
        } catch (IOException e) {
            log.error("持久化复制偏移量失败", e);
        }
    }

    private void applyReplicationMessage(ReplicationMessage msg) throws IOException {
        long seq = msg.getSeq();
        if (seq <= lastAppliedSeq) {
            return;
        }
        switch (msg.getOp()) {
            case PUT -> put(msg.getKey(), msg.getValue());
            case DELETE -> delete(msg.getKey());
        }
        lastAppliedSeq = seq;
        persistLastAppliedSeq(seq);
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
                        Object obj = ois.readObject();
                        if (obj instanceof ReplicationMessage msg) {
                            applyReplicationMessage(msg);
                        }
                    } catch (ClassNotFoundException e) {
                        log.error("反序列化复制消息失败", e);
                    }
                }
            } catch (IOException e) {
                log.error("处理从节点客户端请求异常", e);
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    log.warn("关闭客户端连接失败", e);
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