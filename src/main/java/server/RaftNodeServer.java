package server;

import raft.RaftConfig;
import raft.RaftConfigLoader;
import raft.RaftServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Raft 节点服务端入口。加载配置并启动 Ratis Server。
 * <p>
 * 用法:
 * <pre>
 *   java server.RaftNodeServer                    # 使用默认 raft-config.json
 *   java server.RaftNodeServer my-config.json     # 使用指定配置文件
 * </pre>
 */
public class RaftNodeServer {

    private static final Logger log = LoggerFactory.getLogger(RaftNodeServer.class);

    public static void main(String[] args) {
        // 1. 加载配置
        String configFile = args.length > 0 ? args[0] : "raft-config.json";
        RaftConfig config = RaftConfigLoader.load(configFile);

        log.info("========================================");
        log.info("KV-DB Raft Node Starting");
        log.info("  nodeId   : {}", config.nodeId());
        log.info("  address  : {}", config.getSelfAddress());
        log.info("  dataDir  : {}", config.dataDir());
        log.info("  groupId  : {}", config.raftGroupId());
        log.info("  peers    : {}", config.peers());
        log.info("========================================");

        // 2. 启动 Ratis Server
        try {
            RaftServerBootstrap bootstrap = new RaftServerBootstrap(config);

            // 注册 shutdown hook 优雅关闭
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("收到关闭信号，正在关闭 Ratis Server...");
                try {
                    bootstrap.close();
                    log.info("Ratis Server 已关闭");
                } catch (IOException e) {
                    log.error("关闭 Ratis Server 失败", e);
                }
            }));

            bootstrap.start();
            log.info("Ratis Server 启动成功，等待客户端连接...");
            log.info("按 Ctrl+C 关闭服务器");

            // 阻塞主线程
            Thread.currentThread().join();

        } catch (IOException e) {
            log.error("启动 Ratis Server 失败", e);
            System.exit(1);
        } catch (InterruptedException e) {
            log.info("主线程被中断，退出");
            Thread.currentThread().interrupt();
        }
    }
}
