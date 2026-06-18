package cli;

import raft.RaftConfig;
import raft.RaftConfigLoader;
import raft.RaftKVClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Raft 集群命令行工具。通过 {@link RaftKVClient} 连接到 Raft 集群执行操作。
 * <p>
 * 替代原有的 {@link CommandLineInterface}（直接操作本地引擎），
 * 所有读写请求通过 Raft 共识协议。
 * <p>
 * 用法:
 * <pre>
 *   java cli.RaftCLI                    # 使用默认 raft-config.json
 *   java cli.RaftCLI my-config.json     # 使用指定配置文件
 * </pre>
 */
public class RaftCLI {

    private final RaftKVClient client;

    public RaftCLI(RaftConfig config) {
        this.client = new RaftKVClient(config);
    }

    public void start() throws IOException {
        System.out.println("=== KV-DB Raft CLI ===");
        System.out.println("输入 'help' 查看可用命令，'exit' 退出");
        System.out.println();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        System.out.print("> ");
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                System.out.print("> ");
                continue;
            }

            try {
                if (!processCommand(line)) {
                    break;
                }
            } catch (Exception e) {
                System.err.println("错误: " + e.getMessage());
            }

            System.out.print("> ");
        }

        client.close();
        System.out.println("已退出");
    }

    /**
     * @return true 继续循环，false 退出
     */
    private boolean processCommand(String line) throws IOException {
        String[] parts = line.split("\\s+", 3);
        String cmd = parts[0].toLowerCase();

        switch (cmd) {
            case "put" -> {
                if (parts.length < 3) {
                    System.out.println("用法: put <key> <value>");
                    return true;
                }
                byte[] key = parts[1].getBytes(StandardCharsets.UTF_8);
                String value = parts[2];
                client.put(key, value);
                System.out.println("OK");
            }
            case "get" -> {
                if (parts.length < 2) {
                    System.out.println("用法: get <key>");
                    return true;
                }
                byte[] key = parts[1].getBytes(StandardCharsets.UTF_8);
                Object result = client.get(key);
                if (result == null) {
                    System.out.println("(nil)");
                } else {
                    System.out.println(result);
                }
            }
            case "delete", "del" -> {
                if (parts.length < 2) {
                    System.out.println("用法: delete <key>");
                    return true;
                }
                byte[] key = parts[1].getBytes(StandardCharsets.UTF_8);
                client.delete(key);
                System.out.println("OK");
            }
            case "help" -> {
                System.out.println("可用命令:");
                System.out.println("  put <key> <value>  - 写入键值对");
                System.out.println("  get <key>          - 读取值");
                System.out.println("  delete <key>       - 删除键");
                System.out.println("  help               - 显示帮助");
                System.out.println("  exit               - 退出");
            }
            case "exit", "quit" -> {
                return false;
            }
            default -> {
                System.out.println("未知命令: " + cmd + "，输入 'help' 查看可用命令");
            }
        }
        return true;
    }

    public static void main(String[] args) {
        String configFile = args.length > 0 ? args[0] : "raft-config.json";
        RaftConfig config = RaftConfigLoader.load(configFile);

        System.out.println("连接到 Raft 集群: groupId=" + config.raftGroupId());
        System.out.println("Peers: " + config.peers());
        System.out.println();

        RaftCLI cli = new RaftCLI(config);
        try {
            cli.start();
        } catch (IOException e) {
            System.err.println("CLI 错误: " + e.getMessage());
            System.exit(1);
        }
    }
}
