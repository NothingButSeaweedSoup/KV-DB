package cli;

import cluster.ClusterConfig;
import cluster.ClusterNode;
import util.ConfigLoader;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class CommandLineInterface {
    private final String host;
    private final int port;
    private final PrintStream out = new PrintStream(System.out, true, java.nio.charset.StandardCharsets.UTF_8);

    public CommandLineInterface(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) throws IOException {
        PrintStream out = new PrintStream(System.out, true, java.nio.charset.StandardCharsets.UTF_8);
        try {
            // 加载集群配置
            ClusterConfig config = ConfigLoader.loadConfig();
            // 获取主节点信息
            ClusterNode masterNode = null;
            for (ClusterNode node : config.getNodes()) {
                if (node.isMaster()) {
                    masterNode = node;
                    break;
                }
            }
            if (masterNode == null) {
                out.println("未找到主节点配置。");
                return;
            }
            CommandLineInterface cli = new CommandLineInterface(masterNode.getHost(), masterNode.getPort());
            cli.start();
        } catch (IOException e) {
            out.println("初始化命令行界面失败: " + e.getMessage());
        }
    }

    public void start() {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            Scanner scanner = new Scanner(System.in);
            out.println("欢迎使用CLI工具，输入 'exit' 退出。");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            while (true) {
                System.out.print(">>> ");
                String input = scanner.nextLine().trim();

                if ("exit".equalsIgnoreCase(input)) {
                    break;
                }

                out.println(input);
                String response = in.readLine();
                System.out.println("Server response: " + response);
            }
        } catch (IOException e) {
            out.println("连接到主节点失败: " + e.getMessage());
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        out.println("退出CLI工具。");
    }
}