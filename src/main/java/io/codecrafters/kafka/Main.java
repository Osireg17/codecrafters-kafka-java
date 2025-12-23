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
            logger.info("Kafka mock server listening on port {}", port);
            while (true) {
                clientSocket = serverSocket.accept();
                logger.info("Accepted connection from {}", clientSocket.getRemoteSocketAddress());
                Socket finalClientSocket = clientSocket;
                new Thread(() -> handleClient(finalClientSocket)).start();
            }
        } catch (IOException e) {
            logger.error("IOException in server: {}", e.getMessage(), e);
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                logger.error("IOException while closing server socket: {}", e.getMessage(), e);
            }
        }
    }

    private static void handleClient(Socket clientSocket) {
        try {
            InputStream in = clientSocket.getInputStream();
            OutputStream out = clientSocket.getOutputStream();
            boolean running = true;

            while (running) {

                // READ exactly 4 bytes for message_size.
                byte[] sizeBytes = new byte[4];
                if (!readFully(in, sizeBytes, 0, 4)) {
                    // IF no bytes read or incomplete THEN SET running to false and EXIT loop.
                    running = false;
                    break;
                }

                // PARSE message_size as INT32.
                ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
                int messageSize = sizeBuffer.getInt();

                // Assemble full request: size (4 bytes) + body (messageSize bytes)
                byte[] fullRequest = new byte[4 + messageSize];
                System.arraycopy(sizeBytes, 0, fullRequest, 0, 4);

                // READ exactly message_size bytes for the remainder of the request.
                if (!readFully(in, fullRequest, 4, messageSize)) {
                    // IF read fails or returns fewer bytes THEN EXIT loop.
                    break;
                }

                handleRequest(fullRequest, out);
            }

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

    private static void handleRequest(byte[] requestBytes, OutputStream out) throws IOException {
        logger.info("Received {} bytes from client", requestBytes.length);

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

        out.write(responseBuffer.array());
        out.flush();
    }

    /**
     * Reads exactly the specified number of bytes from the input stream.
     *
     * @return true if all bytes were read, false if EOF or incomplete read
     */
    private static boolean readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int bytesRead = in.read(buffer, offset + totalRead, length - totalRead);
            if (bytesRead == -1) {
                return false; // EOF
            }
            totalRead += bytesRead;
        }
        return true;
    }
}
