package cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated 已被 Apache Ratis 内置集群管理替代。
 *             Ratis 自动处理成员变更、日志复制和 Leader 选举。
 */
@Deprecated
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final List<ClusterNode> nodes;

    public ClusterManager(List<ClusterNode> nodes) {
        this.nodes = nodes;
    }

    /**
     * 同步数据到所有从节点并收集 ACK。
     *
     * @param dataList 待同步的序列化 ReplicationMessage 列表
     * @return 成功确认的从节点数量
     */
    public int syncData(List<byte[]> dataList) {
        int ackCount = 0;
        for (ClusterNode node : nodes) {
            if (!node.isMaster()) {
                if (sendToSlave(node, dataList)) {
                    ackCount++;
                }
            }
        }
        return ackCount;
    }

    /**
     * 向单个从节点发送数据并等待 ACK。
     *
     * @return true 表示从节点确认成功
     */
    /** 协议魔数 */
    private static final byte PROTO_MAGIC = 0x4B; // 'K'
    /** 命令类型：心跳 */
    private static final byte CMD_HEARTBEAT = 0x01;
    /** 命令类型：数据同步 */
    private static final byte CMD_DATA = 0x02;

    private boolean sendToSlave(ClusterNode node, List<byte[]> dataList) {
        try (Socket socket = new Socket(node.getHost(), node.getPort())) {
            socket.setSoTimeout(5000); // 5秒超时
            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {
                // 写入协议头：魔数 + 命令类型
                out.write(PROTO_MAGIC);
                out.write(CMD_DATA);
                // 写入数据列表的大小
                out.write(intToBytes(dataList.size()));
                for (byte[] data : dataList) {
                    out.write(intToBytes(data.length));
                    out.write(data);
                }
                out.flush();

                // 等待 ACK：从节点返回 4 字节状态码
                byte[] ackBuffer = new byte[4];
                int bytesRead = 0;
                while (bytesRead < 4) {
                    int result = in.read(ackBuffer, bytesRead, 4 - bytesRead);
                    if (result == -1) {
                        break;
                    }
                    bytesRead += result;
                }
                if (bytesRead == 4) {
                    int status = bytesToInt(ackBuffer);
                    if (status >= 0) {
                        log.debug("从节点 {}:{} 确认成功, lastAppliedSeq={}",
                                node.getHost(), node.getPort(), status);
                        return true;
                    }
                }
                log.warn("从节点 {}:{} ACK 无效", node.getHost(), node.getPort());
                return false;
            }
        } catch (IOException e) {
            log.error("同步数据到从节点 {}:{} 失败", node.getHost(), node.getPort(), e);
            return false;
        }
    }

    /**
     * 获取所有从节点的状态信息。
     */
    public List<String> getSlaveStatus() {
        List<String> status = new ArrayList<>();
        for (ClusterNode node : nodes) {
            if (!node.isMaster()) {
                status.add(String.format("%s:%d (master=%b)", node.getHost(), node.getPort(), node.isMaster()));
            }
        }
        return status;
    }

    private byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
        };
    }

    private int bytesToInt(byte[] bytes) {
        return (bytes[0] << 24) |
                (bytes[1] << 16) |
                (bytes[2] << 8) |
                (bytes[3] & 0xFF);
    }
}
