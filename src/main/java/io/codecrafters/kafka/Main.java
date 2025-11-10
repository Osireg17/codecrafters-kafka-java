package io.codecrafters.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {

    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);

            serverSocket.setReuseAddress(true);
            clientSocket = serverSocket.accept();
            InputStream in = clientSocket.getInputStream();

            // Read the request from the client
            byte[] requestBytes = new byte[1024];
            int bytesRead = in.read(requestBytes);

            // Parse the request to extract correlation ID
            ByteBuffer request = ByteBuffer.wrap(requestBytes);
            int messageSize = request.getInt();      // bytes 0-3: message size
            short apiKey = request.getShort();       // bytes 4-5: API key
            short apiVersion = request.getShort();   // bytes 6-7: API version
            int correlationId = request.getInt();    // bytes 8-11: correlation ID

            // Build and send the response
            OutputStream out = clientSocket.getOutputStream();
            ByteBuffer response = ByteBuffer.allocate(8);

            response.putInt(0);  // message_size: 0
            response.putInt(correlationId);  // correlation_id (echoed back from request)
            out.write(response.array());
            out.flush();

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
