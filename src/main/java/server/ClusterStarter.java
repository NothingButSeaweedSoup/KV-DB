package server;

import cluster.ClusterConfig;
import cluster.ClusterNode;
import cluster.MasterNode;
import cluster.SlaveNode;
import util.ConfigLoader;

import java.io.IOException;

/**
 * @deprecated 已被 {@link server.RaftNodeServer} 替代。
 *             使用 Raft 共识协议实现分布式集群。
 */
@Deprecated
public class ClusterStarter {
    public static void main(String[] args) throws IOException {
        ClusterConfig config = ConfigLoader.loadConfig();
        for (ClusterNode nodeConfig : config.getNodes()) {
            int port = nodeConfig.getPort();
            boolean isMaster = nodeConfig.isMaster();
            String dataPath = config.getDataPath();

            if (isMaster) {
                new MasterNode(dataPath, config.getNodes()).start();
            } else {
                new SlaveNode(dataPath, port, config).start();
            }
        }
    }
}