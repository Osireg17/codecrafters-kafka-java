package io.codecrafters.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

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

            logger.info("Received {} bytes from client", bytesRead);

            // Parse the request to extract correlation ID
            ByteBuffer request = ByteBuffer.wrap(requestBytes);

            logger.info("Parsing the request {}", request);

            int messageSize = request.getInt();      // bytes 0-3: message size
            short apiKey = request.getShort();       // bytes 4-5: API key
            short apiVersion = request.getShort();   // bytes 6-7: API version
            int correlationId = request.getInt();    // bytes 8-11: correlation ID

            logger.info("Parsed request - messageSize: {}, apiKey: {}, apiVersion: {}, correlationId: {}",
                    messageSize, apiKey, apiVersion, correlationId);

            // Build and send the response
            OutputStream out = clientSocket.getOutputStream();

            // Error response for unsupported API version
            short errorCode = 35; // UNSUPPORTED_VERSION

            // Response: message_size (4 bytes) + correlation_id (4 bytes) + error_code (2 bytes)
            ByteBuffer response = ByteBuffer.allocate(10);

            response.putInt(6);  // message_size: 6 bytes (correlation_id + error_code)
            response.putInt(correlationId);  // correlation_id (echoed back from request)
            response.putShort(errorCode); // error_code as short (2 bytes)

            logger.info("Sending error response with correlationId: {}, errorCode: {}", correlationId, errorCode);

            out.write(response.array());
            out.flush();

        } catch (IOException e) {
            logger.error("IOException: {}", e.getMessage(), e);
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                logger.error("IOException while closing socket: {}", e.getMessage(), e);
            }
        }
    }
}
