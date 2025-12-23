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

        // === BEHAVIOR: Route By apiKey ===
        // 1. IF apiKey equals ApiVersions THEN build ApiVersions response.
        // 2. ELSE IF apiKey equals DescribeTopicPartitions THEN parse request body and build that response.
        // 3. ELSE return an appropriate error or ignore (future stages).
        switch (apiKey) {
            case 18 -> {
                // ApiVersions request
                buildApiVersionsResponse(correlationId, apiVersion, out);
            }
            case 75 -> {
                // DescribeTopicPartitions request
                String topicName = parseDescribeTopicPartitionsRequest(request);
                buildDescribeTopicPartitionsResponse(correlationId, topicName, out);
            }
            default ->
                logger.warn("Unsupported apiKey: {}", apiKey);
        }
    }

    private static void buildApiVersionsResponse(int correlationId, short apiVersion, OutputStream out) throws IOException {
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

        // Construct ApiVersions response
        int body_size = 2 + 1 + (2 + 2 + 2 + 1) + (2 + 2 + 2 + 1) + 4 + 1;
        // body_size breakdown:
        int message_size = 4 + body_size; // correlation_id (4 bytes) + body_size
        ByteBuffer responseBuffer = ByteBuffer.allocate(4 + message_size);
        responseBuffer.putInt(message_size);  // message_size
        responseBuffer.putInt(correlationId); // correlation_id
        responseBuffer.putShort(error_code);   // error_code
        // 1. WRITE compact array length (2 entries => length+1 = 3).
        responseBuffer.put((byte) 3);         // api_keys length (compact array length = 2 + 1)
        responseBuffer.putShort((short) api_key); // api_key
        responseBuffer.putShort(min_version);      // min_version
        responseBuffer.putShort(max_version);      // max_version
        responseBuffer.put((byte) 0);         // tagged_fields (empty)
        // 2. WRITE DescribeTopicPartitions entry (api_key=75, min=0, max=0, tagged_fields empty).
        responseBuffer.putShort((short) 75);  // api_key (DescribeTopicPartitions)
        responseBuffer.putShort((short) 0);   // min_version
        responseBuffer.putShort((short) 0);   // max_version
        responseBuffer.put((byte) 0);         // tagged_fields (empty)
        responseBuffer.putInt(0);             // throttle_time_ms
        responseBuffer.put((byte) 0);         // tagged_fields (empty)

        logger.info("Sending ApiVersions response with correlationId: {}", correlationId);
        out.write(responseBuffer.array());
        out.flush();
    }

    private static String parseDescribeTopicPartitionsRequest(ByteBuffer requestBuffer) {

        logger.info("Starting to parse DescribeTopicPartitions request. Buffer position: {}, remaining: {}",
            requestBuffer.position(), requestBuffer.remaining());

        // Skip client_id (nullable string - INT16 length prefix)
        short clientIdLength = requestBuffer.getShort();
        logger.info("Client ID length (INT16): {}", clientIdLength);

        if (clientIdLength > 0) {
            requestBuffer.position(requestBuffer.position() + clientIdLength);
            logger.info("Skipped {} bytes for client_id. New position: {}", clientIdLength, requestBuffer.position());
        }

        // Skip header TAG_BUFFER (expect empty)
        byte headerTagBuffer = requestBuffer.get();
        logger.info("Header TAG_BUFFER: 0x{}, position: {}",
            String.format("%02X", headerTagBuffer), requestBuffer.position());

        // Now read topics array
        byte topicsArrayLengthByte = requestBuffer.get();
        int topicsArrayLength = Byte.toUnsignedInt(topicsArrayLengthByte);
        logger.info("Topics array length byte: 0x{}, computed length: {}, position: {}",
            String.format("%02X", topicsArrayLengthByte), topicsArrayLength, requestBuffer.position());

        if (topicsArrayLength <= 1) {
            logger.warn("Topics array length is <= 1, returning empty string");
            return ""; // No topics
        }

        // Read topic_name
        byte topicNameLengthByte = requestBuffer.get();
        int topicNameLength = Byte.toUnsignedInt(topicNameLengthByte) - 1;
        logger.info("Topic name length byte: 0x{}, computed length: {}, position: {}",
            String.format("%02X", topicNameLengthByte), topicNameLength, requestBuffer.position());

        byte[] topicNameBytes = new byte[topicNameLength];
        requestBuffer.get(topicNameBytes);
        String topicName = new String(topicNameBytes, java.nio.charset.StandardCharsets.UTF_8);
        logger.info("Extracted topic name: '{}', position: {}", topicName, requestBuffer.position());

        // Skip topic TAG_BUFFER (expect empty)
        byte topicTagBuffer = requestBuffer.get();
        logger.info("Topic TAG_BUFFER: 0x{}, position: {}",
            String.format("%02X", topicTagBuffer), requestBuffer.position());

        // Skip ResponsePartitionLimit (INT32, 4 bytes)
        int responsePartitionLimit = requestBuffer.getInt();
        logger.info("ResponsePartitionLimit: {}, position: {}", responsePartitionLimit, requestBuffer.position());

        // Skip Cursor (nullable byte)
        byte cursor = requestBuffer.get();
        logger.info("Cursor: 0x{} ({}), position: {}",
            String.format("%02X", cursor), cursor, requestBuffer.position());

        // Skip request TAG_BUFFER (expect empty)
        byte requestTagBuffer = requestBuffer.get();
        logger.info("Request TAG_BUFFER: 0x{}, position: {}",
            String.format("%02X", requestTagBuffer), requestBuffer.position());

        logger.info("Finished parsing. Returning topic name: '{}'", topicName);
        return topicName;

    }

    private static void buildDescribeTopicPartitionsResponse(int correlationId, String topicName, OutputStream out) throws IOException {

        short error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
        byte[] topicNameBytes = topicName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int topicNameByteLength = topicNameBytes.length;

        // Header v1: correlation_id (4) + TAG_BUFFER (1) = 5 bytes
        // Body: throttle_time_ms (4) + topics array (1 + topic_entry) + next_cursor (1) + TAG_BUFFER (1)
        // Topic entry: error_code (2) + topic_name (1 + length) + topic_id (16) + is_internal (1) + partitions (1) + authorized_ops (4) + TAG_BUFFER (1)
        int body_size = 4 + 1 + (2 + topicNameByteLength + 1 + 16 + 1 + 1 + 4 + 1) + 1 + 1;
        int message_size = 4 + 1 + body_size; // correlation_id (4) + header TAG_BUFFER (1) + body_size

        ByteBuffer responseBuffer = ByteBuffer.allocate(4 + message_size);
        responseBuffer.putInt(message_size);  // message_size
        responseBuffer.putInt(correlationId); // correlation_id
        responseBuffer.put((byte) 0);         // header TAG_BUFFER (empty)
        responseBuffer.putInt(0);             // throttle_time_ms
        responseBuffer.put((byte) 2);         // topics length (compact array length = 1 + 1)

        // Topic entry
        responseBuffer.putShort(error_code);   // error_code
        responseBuffer.put((byte) (topicNameByteLength + 1)); // topic_name length (compact string length = byte_length + 1)
        responseBuffer.put(topicNameBytes); // topic_name
        // topic_id = all zero UUID
        for (int i = 0; i < 16; i++) {
            responseBuffer.put((byte) 0);
        }
        responseBuffer.put((byte) 0);         // is_internal = false
        responseBuffer.put((byte) 1);         // partitions length (compact array length = 0 + 1)
        responseBuffer.putInt(0);             // topic_authorized_operations
        responseBuffer.put((byte) 0);         // topic TAG_BUFFER empty

        responseBuffer.put((byte) -1);        // next_cursor = -1 (null, NULLABLE_INT8 = 1 byte)
        responseBuffer.put((byte) 0);         // response TAG_BUFFER empty

        logger.info("Sending DescribeTopicPartitions response for topic: {} with correlationId: {}", topicName, correlationId);

        out.write(responseBuffer.array());
        out.flush();
    }

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
