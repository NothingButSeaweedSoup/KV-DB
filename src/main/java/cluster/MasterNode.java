package cluster;

import core.LSMStorageEngine;
import util.ByteUtil;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class MasterNode extends LSMStorageEngine {
    private ClusterManager clusterManager;
    private ServerSocket serverSocket;

    public MasterNode(String dataPath, List<ClusterNode> nodes) throws IOException {
        super(dataPath);
        this.clusterManager = new ClusterManager(nodes);
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(12345);
        System.out.println("Master node is running on port 12345");

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from " + clientSocket.getInetAddress().getHostAddress());

                // 处理客户端请求
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } finally {
            serverSocket.close();
        }
    }

    @Override
    public void put(byte[] key, Object value) throws IOException {
        super.put(key, value);
        byte[] data = serializeData(key, value);
        clusterManager.syncData(data);
    }

    private byte[] serializeData(byte[] key, Object value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        return baos.toByteArray();
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
                            } else {
                                out.println("Usage: put <key> <value>");
                            }
                            break;
                        case "delete":
                            if (key != null) {
                                databaseDelete(key);
                                out.println("OK");
                            } else {
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
            Object result = get(key.getBytes());
            return result != null ? result.toString() : "null";
        }

        private void databasePut(String key, String value) throws IOException {
            put(key.getBytes(), value);
        }

        private void databaseDelete(String key) throws IOException {
            delete(key.getBytes());
        }

        private void databaseFlush() throws IOException {
            flush();
        }
    }
}