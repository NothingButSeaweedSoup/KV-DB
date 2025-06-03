package cluster;

import java.util.List;

public class ClusterConfig {
    private List<ClusterNode> nodes;
    private String dataPath;

    public List<ClusterNode> getNodes() {
        return nodes;
    }

    public String getDataPath() {
        return dataPath;
    }
}