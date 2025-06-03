package cli;

import core.LSMStorageEngine;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class CommandLineInterface {
    private final LSMStorageEngine storageEngine;
    private final PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);

    public CommandLineInterface(String dataPath) throws IOException {
        this.storageEngine = new LSMStorageEngine(dataPath);
    }

    public static void main(String[] args) {
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        try {
            CommandLineInterface cli = new CommandLineInterface("data");
            cli.start();
        } catch (IOException e) {
            out.println("初始化存储引擎失败: " + e.getMessage());
        }
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        out.println("欢迎使用CLI工具，输入 'exit' 退出。");

        while (true) {
            System.out.print(">>> ");
            String input = scanner.nextLine().trim();

            if ("exit".equalsIgnoreCase(input)) {
                break;
            }

            String[] parts = input.split(" ", 2);
            if (parts.length < 1) {
                out.println("无效的命令。");
                continue;
            }

            String command = parts[0].toLowerCase();
            switch (command) {
                case "put":
                    if (parts.length < 2) {
                        out.println("PUT命令需要键和值。");
                        break;
                    }
                    String[] keyValue = parts[1].split(" ", 2);
                    if (keyValue.length < 2) {
                        out.println("PUT命令需要键和值。");
                        break;
                    }
                    try {
                        storageEngine.put(keyValue[0].getBytes(), keyValue[1]);
                        out.println("插入或更新成功。");
                    } catch (IOException e) {
                        out.println("插入或更新失败: " + e.getMessage());
                    }
                    break;

                case "get":
                    if (parts.length < 2) {
                        out.println("GET命令需要键。");
                        break;
                    }
                    try {
                        Object value = storageEngine.get(parts[1].getBytes());
                        if (value != null) {
                            out.println("值: " + value);
                        } else {
                            out.println("键不存在。");
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        out.println("查询失败: " + e.getMessage());
                    }
                    break;

                case "delete":
                    if (parts.length < 2) {
                        out.println("DELETE命令需要键。");
                        break;
                    }
                    try {
                        storageEngine.delete(parts[1].getBytes());
                        out.println("删除成功。");
                    } catch (IOException e) {
                        out.println("删除失败: " + e.getMessage());
                    }
                    break;

                case "flush":
                    try {
                        storageEngine.flush();
                        out.println("数据已刷新到磁盘。");
                    } catch (IOException e) {
                        out.println("刷新数据失败: " + e.getMessage());
                    }
                    break;

                case "help":
                    out.println("支持的命令:");
                    out.println("  put <key> <value>: 插入或更新键值对。");
                    out.println("  get <key>: 查询键对应的值。");
                    out.println("  delete <key>: 删除键对应的值。");
                    out.println("  flush: 刷新数据到磁盘。");
                    out.println("  help: 显示帮助信息。");
                    out.println("  exit: 退出CLI工具。");
                    break;

                default:
                    out.println("未知的命令。请输入 'help' 获取帮助。");
                    break;
            }
        }

        try {
            storageEngine.close();
        } catch (IOException e) {
            out.println("关闭存储引擎失败: " + e.getMessage());
        }
        out.println("退出CLI工具。");
    }
}