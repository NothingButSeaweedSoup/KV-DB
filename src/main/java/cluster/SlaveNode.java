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
                // 读取数据列表的大小
                int dataSize = bytesToInt(readBytes(in, 4));
                for (int i = 0; i < dataSize; i++) {
                    // 读取每个数据的长度
                    int dataLength = bytesToInt(readBytes(in, 4));
                    // 读取数据本身
                    byte[] data = readBytes(in, dataLength);
                    // 处理接收到的数据
                    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                        Object value = ois.readObject();
                        // 更新本地存储
                        put("received_key".getBytes(), value);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
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

        // 辅助方法：从InputStream中读取指定长度的字节
        private byte[] readBytes(InputStream in, int length) throws IOException {
            byte[] buffer = new byte[length];
            int bytesRead = 0;
            while (bytesRead < length) {
                int result = in.read(buffer, bytesRead, length - bytesRead);
                if (result == -1) {
                    throw new IOException("Unexpected end of stream");
                }
                bytesRead += result;
            }
            return buffer;
        }

        // 辅助方法：将4字节的byte数组转换为int
        private int bytesToInt(byte[] bytes) {
            return (bytes[0] << 24) |
                    (bytes[1] << 16) |
                    (bytes[2] << 8) |
                    (bytes[3] & 0xFF);
        }
    }
}