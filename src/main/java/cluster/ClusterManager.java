package cluster;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClusterManager {
    private List<ClusterNode> nodes;

    public ClusterManager(List<ClusterNode> nodes) {
        this.nodes = nodes;
    }

    public void syncData(byte[] data) {
        for (ClusterNode node : nodes) {
            if (!node.isMaster()) {
                try (Socket socket = new Socket(node.getHost(), node.getPort())) {
                    try (OutputStream out = socket.getOutputStream()) {
                        out.write(data);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}