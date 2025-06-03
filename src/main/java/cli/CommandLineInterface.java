package cli;

import core.LSMStorageEngine;

import java.io.IOException;
import java.util.Scanner;

public class CommandLineInterface {
    private final LSMStorageEngine storageEngine;

    public CommandLineInterface(String dataPath) throws IOException {
        this.storageEngine = new LSMStorageEngine(dataPath);
    }

    public static void main(String[] args) {
        try {
            CommandLineInterface cli = new CommandLineInterface("data");
            cli.start();
        } catch (IOException e) {
            System.out.println("初始化存储引擎失败: " + e.getMessage());
        }
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("欢迎使用CLI工具，输入 'exit' 退出。");

        while (true) {
            System.out.print(">>> ");
            String input = scanner.nextLine().trim();

            if ("exit".equalsIgnoreCase(input)) {
                break;
            }

            String[] parts = input.split(" ", 2);
            if (parts.length < 1) {
                System.out.println("无效的命令。");
                continue;
            }

            String command = parts[0].toLowerCase();
            switch (command) {
                case "put":
                    if (parts.length < 2) {
                        System.out.println("PUT命令需要键和值。");
                        break;
                    }
                    String[] keyValue = parts[1].split(" ", 2);
                    if (keyValue.length < 2) {
                        System.out.println("PUT命令需要键和值。");
                        break;
                    }
                    try {
                        storageEngine.put(keyValue[0].getBytes(), keyValue[1]);
                        System.out.println("插入或更新成功。");
                    } catch (IOException e) {
                        System.out.println("插入或更新失败: " + e.getMessage());
                    }
                    break;

                case "get":
                    if (parts.length < 2) {
                        System.out.println("GET命令需要键。");
                        break;
                    }
                    try {
                        Object value = storageEngine.get(parts[1].getBytes());
                        if (value != null) {
                            System.out.println("值: " + value);
                        } else {
                            System.out.println("键不存在。");
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        System.out.println("查询失败: " + e.getMessage());
                    }
                    break;

                case "delete":
                    if (parts.length < 2) {
                        System.out.println("DELETE命令需要键。");
                        break;
                    }
                    try {
                        storageEngine.delete(parts[1].getBytes());
                        System.out.println("删除成功。");
                    } catch (IOException e) {
                        System.out.println("删除失败: " + e.getMessage());
                    }
                    break;

                case "flush":
                    try {
                        storageEngine.flush();
                        System.out.println("数据已刷新到磁盘。");
                    } catch (IOException e) {
                        System.out.println("刷新数据失败: " + e.getMessage());
                    }
                    break;

                case "help":
                    System.out.println("支持的命令:");
                    System.out.println("  put <key> <value>: 插入或更新键值对。");
                    System.out.println("  get <key>: 查询键对应的值。");
                    System.out.println("  delete <key>: 删除键对应的值。");
                    System.out.println("  flush: 刷新数据到磁盘。");
                    System.out.println("  help: 显示帮助信息。");
                    System.out.println("  exit: 退出CLI工具。");
                    break;

                default:
                    System.out.println("未知的命令。请输入 'help' 获取帮助。");
                    break;
            }
        }

        try {
            storageEngine.close();
        } catch (IOException e) {
            System.out.println("关闭存储引擎失败: " + e.getMessage());
        }

        System.out.println("退出CLI工具。");
    }
}