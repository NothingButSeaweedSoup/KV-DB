package cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterNode {
    private String host;
    private int port;
    private boolean master;

    @JsonCreator
    public ClusterNode(@JsonProperty("host") String host, @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
    }

    public ClusterNode() { }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    @JsonProperty("isMaster")
    public boolean isMaster() { return master; }

    @JsonProperty("isMaster")
    public void setMaster(boolean master) { this.master = master; }
}
