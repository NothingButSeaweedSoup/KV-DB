package server;

import core.LSMStorageEngine;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class DBServer {
    private ServerSocket serverSocket;
    private int port;
    private LSMStorageEngine db;
    private boolean isMaster;

    public DBServer(int port, boolean isMaster) throws IOException {
        this.port = port;
        this.isMaster = isMaster;
        this.db = new LSMStorageEngine("data");
    }

    public static void main(String[] args) throws IOException {
        int masterPort = 12345;
        int slavePort1 = 12346;
        int slavePort2 = 12347;

        new Thread(() -> {
            try {
                new DBServer(masterPort, true).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                new DBServer(slavePort1, false).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                new DBServer(slavePort2, false).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println((isMaster ? "Master" : "Slave") + " server is running on port " + port);

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from " + clientSocket.getInetAddress().getHostAddress());

                new Thread(new ClientHandler(clientSocket, isMaster)).start();
            }
        } finally {
            serverSocket.close();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;
        private boolean isMaster;

        public ClientHandler(Socket socket, boolean isMaster) {
            this.clientSocket = socket;
            this.isMaster = isMaster;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    if (isMaster) {
                        // 主节点处理客户端请求
                        String[] parts = inputLine.split(" ", 3);
                        String operation = parts[0].toLowerCase();
                        String key = parts.length > 1 ? parts[1] : null;
                        String value = parts.length > 2 ? parts[2] : null;

                        switch (operation) {
                            case "get":
                                Object result = db.get(key.getBytes(StandardCharsets.UTF_8));
                                out.println(result != null ? result.toString() : "null");
                                break;
                            case "put":
                                if (key != null && value != null) {
                                    db.put(key.getBytes(StandardCharsets.UTF_8), value);
                                    out.println("OK");
                                } else {
                                    out.println("Usage: put <key> <value>");
                                }
                                break;
                            case "delete":
                                if (key != null) {
                                    db.delete(key.getBytes(StandardCharsets.UTF_8));
                                    out.println("OK");
                                } else {
                                    out.println("Usage: delete <key>");
                                }
                                break;
                            case "exit":
                                out.println("Server is shutting down.");
                                break;
                            default:
                                out.println("ERROR Unknown operation");
                                break;
                        }
                    } else {
                        // 从节点处理主节点同步的数据
                        byte[] data = inputLine.getBytes(StandardCharsets.UTF_8);
                        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                            Object value = ois.readObject();
                            db.put("received_key".getBytes(StandardCharsets.UTF_8), value);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
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
    }
}