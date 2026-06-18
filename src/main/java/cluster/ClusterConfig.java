package cluster;

import java.util.List;

public class ClusterConfig {
    private List<ClusterNode> nodes;
    private String dataPath;
    private String replicationMode = "SYNC";

    public List<ClusterNode> getNodes() {
        return nodes;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getReplicationMode() {
        return replicationMode;
    }

    public void setReplicationMode(String replicationMode) {
        this.replicationMode = replicationMode;
    }
}