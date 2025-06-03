package client;

import java.io.*;
import java.net.*;

public class DBClient {
    private Socket socket;
    private String host;
    private int port;

    public DBClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws IOException {
        socket = new Socket(host, port);
        System.out.println("Connected to server at " + host + ":" + port);

        new Thread(new ClientHandler()).start();
    }

    private class ClientHandler implements Runnable {
        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in))) {

                String inputLine;
                while ((inputLine = userInput.readLine()) != null) {
                    out.println(inputLine);
                    String response = in.readLine();
                    System.out.println("Server response: " + response);

                    if ("Server is shutting down.".equals(response)) {
                        System.out.println("Connection closed by server.");
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        String host = "localhost"; // 服务器的IP地址
        int port = 12345; // 服务器监听的端口号
        DBClient client = new DBClient(host, port);
        try {
            client.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}