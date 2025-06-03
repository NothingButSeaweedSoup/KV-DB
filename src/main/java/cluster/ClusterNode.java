package cluster;

public class ClusterNode {
    private String host;
    private int port;
    private boolean isMaster;

    public ClusterNode(String host, int port, boolean isMaster) {
        this.host = host;
        this.port = port;
        this.isMaster = isMaster;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isMaster() {
        return isMaster;
    }
}