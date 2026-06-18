package cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 心跳管理器。
 * <p>
 * 主节点定期向从节点发送心跳检测，维护活跃从节点列表。
 * 对每个从节点复用持久连接，连接断开时自动重连。
 */
/**
 * @deprecated 已被 Apache Ratis 内置心跳机制替代。
 *             Ratis Leader 自动向 Follower 发送心跳。
 */
@Deprecated
public class HeartbeatManager {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatManager.class);

    private static final int HEARTBEAT_INTERVAL_MS = 5000;
    private static final int HEARTBEAT_TIMEOUT_MS = 3000;
    /** 协议魔数 */
    private static final byte PROTO_MAGIC = 0x4B; // 'K'
    /** 命令类型：心跳 */
    private static final byte CMD_HEARTBEAT = 0x01;

    private final List<ClusterNode> nodes;
    private final ScheduledExecutorService scheduler;
    private final CopyOnWriteArrayList<ClusterNode> activeSlaves = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, Socket> connectionPool = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public HeartbeatManager(List<ClusterNode> nodes) {
        this.nodes = nodes;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动心跳检测。
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleAtFixedRate(this::checkAllSlaves,
                    0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
            log.info("心跳检测已启动，间隔: {}ms", HEARTBEAT_INTERVAL_MS);
        }
    }

    /**
     * 停止心跳检测并关闭所有连接。
     */
    public void stop() {
        running.set(false);
        scheduler.shutdown();
        closeAllConnections();
    }

    /**
     * 获取当前活跃的从节点列表。
     */
    public List<ClusterNode> getActiveSlaves() {
        return List.copyOf(activeSlaves);
    }

    /**
     * 获取活跃从节点数量。
     */
    public int getActiveSlaveCount() {
        return activeSlaves.size();
    }

    private void checkAllSlaves() {
        for (ClusterNode node : nodes) {
            if (node.isMaster()) {
                continue;
            }
            boolean alive = sendHeartbeat(node);
            if (alive && !activeSlaves.contains(node)) {
                activeSlaves.add(node);
                log.info("从节点上线: {}:{}", node.getHost(), node.getPort());
            } else if (!alive && activeSlaves.contains(node)) {
                activeSlaves.remove(node);
                log.warn("从节点离线: {}:{}", node.getHost(), node.getPort());
            }
        }
    }

    private boolean sendHeartbeat(ClusterNode node) {
        String poolKey = node.getHost() + ":" + node.getPort();
        Socket socket = connectionPool.get(poolKey);

        // 检查现有连接是否可用
        if (socket != null && (socket.isClosed() || socket.isInputShutdown())) {
            closeQuietly(socket);
            connectionPool.remove(poolKey);
            socket = null;
        }

        try {
            if (socket == null) {
                socket = new Socket(node.getHost(), node.getPort());
                socket.setSoTimeout(HEARTBEAT_TIMEOUT_MS);
                socket.setKeepAlive(true);
                connectionPool.put(poolKey, socket);
            }

            OutputStream out = socket.getOutputStream();
            out.write(PROTO_MAGIC);
            out.write(CMD_HEARTBEAT);
            out.flush();

            InputStream in = socket.getInputStream();
            int respMagic = in.read();
            int respCmd = in.read();

            if (respMagic == PROTO_MAGIC && respCmd == CMD_HEARTBEAT) {
                return true;
            }
            // 响应异常，关闭连接
            closeQuietly(socket);
            connectionPool.remove(poolKey);
            return false;

        } catch (IOException e) {
            // 连接失败，清理
            if (socket != null) {
                closeQuietly(socket);
                connectionPool.remove(poolKey);
            }
            return false;
        }
    }

    private void closeAllConnections() {
        connectionPool.forEach((key, socket) -> closeQuietly(socket));
        connectionPool.clear();
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
