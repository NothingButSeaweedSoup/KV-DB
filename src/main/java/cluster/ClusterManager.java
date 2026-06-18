package cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private List<ClusterNode> nodes;

    public ClusterManager(List<ClusterNode> nodes) {
        this.nodes = nodes;
    }

    // 修改syncData方法以接受List<byte[]>类型的参数
    public void syncData(List<byte[]> dataList) {
        for (ClusterNode node : nodes) {
            if (!node.isMaster()) {
                try (Socket socket = new Socket(node.getHost(), node.getPort())) {
                    try (OutputStream out = socket.getOutputStream()) {
                        // 写入数据列表的大小
                        out.write(intToBytes(dataList.size()));
                        for (byte[] data : dataList) {
                            // 写入每个数据的长度
                            out.write(intToBytes(data.length));
                            // 写入数据本身
                            out.write(data);
                        }
                    }
                } catch (IOException e) {
                    log.error("同步数据到从节点 {}:{} 失败", node.getHost(), node.getPort(), e);
                }
            }
        }
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