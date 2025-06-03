package cluster;

import core.LSMStorageEngine;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SlaveNode extends LSMStorageEngine {
    private ServerSocket serverSocket;

    public SlaveNode(String dataPath, int port) throws IOException {
        super(dataPath);
        this.serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new ClientHandler(socket)).start();
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
            try (InputStream in = clientSocket.getInputStream()) {
                byte[] data = new byte[1024];
                int bytesRead;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                while ((bytesRead = in.read(data)) != -1) {
                    baos.write(data, 0, bytesRead);
                }
                byte[] receivedData = baos.toByteArray();
                // 处理接收到的数据
                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(receivedData))) {
                    Object value = ois.readObject();
                    // 更新本地存储
                    put("received_key".getBytes(), value);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
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