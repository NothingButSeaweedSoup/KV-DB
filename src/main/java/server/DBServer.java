package server;

import core.LSMStorageEngine;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class DBServer {
    private ServerSocket serverSocket;
    private int port;
    private LSMStorageEngine db;

    public DBServer(int port) throws IOException {
        this.port = port;
        db = new LSMStorageEngine("data");
    }

    public static void main(String[] args) throws IOException {
        int port = 12345; // 服务器监听的端口号
        DBServer server = new DBServer(port);
        server.start();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("Server is running on port " + port);

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from " + clientSocket);

                Thread thread = new Thread(new ClientHandler(clientSocket));
                thread.start();
            }
        } finally {
            serverSocket.close();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    // 解析客户端请求
                    String[] parts = inputLine.split(" ", 3);
                    String operation = parts[0].toLowerCase();
                    String key = parts.length > 1 ? parts[1] : null;
                    String value = parts.length > 2 ? parts[2] : null;

                    // 执行数据库操作
                    switch (operation) {
                        case "get":
                            String result = "Usage: get <key>";
                            if (key != null) {
                                result = databaseGet(key);
                            }
                            out.println(result);
                            break;
                        case "put":
                            if (key != null && value != null) {
                                databasePut(key, value);
                                out.println("OK");
                            }else {
                                out.println("Usage: put <key> <value>");
                            }
                            break;
                        case "delete":
                            if (key != null) {
                                databaseDelete(key);
                                out.println("OK");
                            }else {
                                out.println("Usage: delete <key>");
                            }
                            break;
                        case "flush":
                            databaseFlush();
                            out.println("OK");
                            break;
                        case "exit":
                            out.println("Server is shutting down.");
                            break;
                        case "help":
                            out.println("Supported commands: get, put, delete, flush, exit, help");
                            break;
                        default:
                            out.println("ERROR Unknown operation");
                            break;
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private String databaseGet(String key) throws IOException, ClassNotFoundException {
            Object result = db.get(key.getBytes(StandardCharsets.UTF_8));
            return result != null ? result.toString() : "null";
        }

        private void databasePut(String key, String value) throws IOException {
            db.put(key.getBytes(StandardCharsets.UTF_8), value);
        }

        private void databaseDelete(String key) throws IOException {
            db.delete(key.getBytes(StandardCharsets.UTF_8));
        }

        private void databaseFlush() throws IOException {
            db.flush();
        }
    }
}