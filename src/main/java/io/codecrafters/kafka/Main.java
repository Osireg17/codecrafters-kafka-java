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

            int api_key = 18;
            short min_version = 0;
            short max_version = 4;

            // Determine error_code based on requested API version
            short error_code;
            if (apiVersion < min_version || apiVersion > max_version) {
                error_code = 35; // UNSUPPORTED_VERSION
            } else {
                error_code = 0;  // Success
            }

            int body_size = 2 + 1 + (2 + 2 + 2 + 1) + 4 + 1;
            int message_size = 4 + body_size;
            ByteBuffer responseBuffer = ByteBuffer.allocate(4 + message_size);
            responseBuffer.putInt(message_size);  // message_size
            responseBuffer.putInt(correlationId); // correlation_id
            responseBuffer.putShort(error_code);   // error_code
            responseBuffer.put((byte) 2);         // api_keys length (compact array length = 1 + 1)
            responseBuffer.putShort((short) api_key); // api_key
            responseBuffer.putShort(min_version);      // min_version
            responseBuffer.putShort(max_version);      // max_version
            responseBuffer.put((byte) 0);         // tagged_fields (empty)
            responseBuffer.putInt(0);             // throttle_time_ms
            responseBuffer.put((byte) 0);         // tagged_fields (empty)  
            logger.info("Sending ApiVersions response with correlationId: {}", correlationId);

            OutputStream out = clientSocket.getOutputStream();
            out.write(responseBuffer.array());
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
