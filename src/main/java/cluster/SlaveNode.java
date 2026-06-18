package cluster;

import core.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;

/**
 * @deprecated 已被 {@link raft.RaftServerBootstrap} + {@link raft.KvStoreStateMachine} 替代。
 *             Ratis Follower 节点自动接收日志复制，无需手动管理。
 */
@Deprecated
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

    /** 协议魔数，标识有效命令 */
    private static final byte PROTO_MAGIC = 0x4B; // 'K'
    /** 命令类型：心跳 */
    private static final byte CMD_HEARTBEAT = 0x01;
    /** 命令类型：数据同步 */
    private static final byte CMD_DATA = 0x02;

    public void start() throws IOException {
        log.info("从节点启动，监听端口 {}", serverSocket.getLocalPort());
        try {
            while (true) {
                Socket socket = serverSocket.accept();
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
            try (InputStream in = clientSocket.getInputStream();
                 OutputStream out = clientSocket.getOutputStream()) {
                // 读取魔数 (1 byte)
                int magic = in.read();
                if (magic != PROTO_MAGIC) {
                    log.warn("收到无效协议魔数: 0x{}", Integer.toHexString(magic & 0xFF));
                    return;
                }

                // 读取命令类型 (1 byte)
                int cmdType = in.read();
                if (cmdType == CMD_HEARTBEAT) {
                    // 心跳响应：魔数 + 命令类型
                    out.write(PROTO_MAGIC);
                    out.write(CMD_HEARTBEAT);
                    out.flush();
                    return;
                }

                if (cmdType != CMD_DATA) {
                    log.warn("收到未知命令类型: 0x{}", Integer.toHexString(cmdType & 0xFF));
                    return;
                }

                // 数据同步：读取数据列表大小 (4 bytes)
                int dataSize = bytesToInt(readBytes(in, 4));

                for (int i = 0; i < dataSize; i++) {
                    // 读取每个数据的长度
                    int dataLength = bytesToInt(readBytes(in, 4));
                    // 读取数据本身
                    byte[] data = readBytes(in, dataLength);
                    // 处理接收到的数据
                    try {
                        ReplicationMessage msg = ReplicationCodec.decode(data);
                        applyReplicationMessage(msg);
                    } catch (Exception e) {
                        log.error("反序列化复制消息失败", e);
                    }
                }
                // 返回 ACK：lastAppliedSeq（4字节）
                out.write(intToBytes((int) lastAppliedSeq));
                out.flush();
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
            readBytesInto(in, buffer, 0, length);
            return buffer;
        }

        // 辅助方法：从InputStream中读取字节到指定位置
        private void readBytesInto(InputStream in, byte[] buffer, int offset, int length) throws IOException {
            int bytesRead = 0;
            while (bytesRead < length) {
                int result = in.read(buffer, offset + bytesRead, length - bytesRead);
                if (result == -1) {
                    throw new IOException("Unexpected end of stream");
                }
                bytesRead += result;
            }
        }

        // 辅助方法：将4字节的byte数组转换为int
        private int bytesToInt(byte[] bytes) {
            return (bytes[0] << 24) |
                    (bytes[1] << 16) |
                    (bytes[2] << 8) |
                    (bytes[3] & 0xFF);
        }

        // 辅助方法：将int转换为4字节的byte数组
        private byte[] intToBytes(int value) {
            return new byte[]{
                    (byte) (value >> 24),
                    (byte) (value >> 16),
                    (byte) (value >> 8),
                    (byte) value
            };
        }
    }
}