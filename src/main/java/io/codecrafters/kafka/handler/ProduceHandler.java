package io.codecrafters.kafka.handler;

import io.codecrafters.kafka.common.Topic;
import io.codecrafters.kafka.protocol.KafkaRequest;
import io.codecrafters.kafka.protocol.KafkaResponse;
import io.codecrafters.kafka.protocol.ProduceRequestParser;
import io.codecrafters.kafka.protocol.ProduceResponseBuilder;
import io.codecrafters.kafka.storage.PartitionLogReader;
import io.codecrafters.kafka.storage.TopicLogReader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Handler for Produce requests (API key 0, version 11).
 * Validates topic and partition existence against __cluster_metadata and returns appropriate response.
 */
public class ProduceHandler implements RequestHandler {

    private final TopicLogReader topicLogReader;
    private final PartitionLogReader partitionLogReader;

    public ProduceHandler(String dataLogPath, String logFileName) {
        this.topicLogReader = new TopicLogReader(dataLogPath + "/__cluster_metadata-0", logFileName);
        this.partitionLogReader = new PartitionLogReader(
                dataLogPath,
                logFileName
        );
    }

    @Override
    public KafkaResponse handle(KafkaRequest request) {
        ByteBuffer buffer = request.getRawBuffer();

        try {
            // Parse Produce request to extract topic name and partition index
            ProduceRequestParser.ProduceTopic produceTopic = ProduceRequestParser.parseTopic(buffer);

            if (produceTopic == null) {
                System.err.println("Failed to parse Produce request - no topics or partitions found");
                return handleEmptyRequest(request.getCorrelationId());
            }

            String topicName = produceTopic.getTopicName();
            int partitionIndex = produceTopic.getPartitionIndex();

            System.out.println("Produce request for topic: " + topicName + ", partition: " + partitionIndex);

            // Validate topic and partition
            int errorCode = validateTopicAndPartition(topicName, partitionIndex);

            // Build response based on validation result
            ProduceResponseBuilder builder = new ProduceResponseBuilder(request.getCorrelationId());

            if (errorCode == 0) {
                System.out.println("Topic and partition validated successfully");
                builder.buildSuccessResponse(topicName, partitionIndex);
            } else {
                System.out.println("Topic or partition not found, returning error code: " + errorCode);
                builder.buildErrorResponse(topicName, partitionIndex);
            }

            return builder.build();

        } catch (Exception e) {
            System.err.println("Error parsing Produce request: " + e.getMessage());
            e.printStackTrace();
            return handleEmptyRequest(request.getCorrelationId());
        }
    }

    /**
     * Validate if the requested topic and partition exist in the cluster metadata.
     *
     * @param topicName The name of the topic to validate
     * @param partitionIndex The partition index to validate
     * @return 0 if valid, 3 (UNKNOWN_TOPIC_OR_PARTITION) if invalid
     */
    private int validateTopicAndPartition(String topicName, int partitionIndex) {
        // Step 1: Load topicMap from metadata log
        Map<String, Topic> topicMap = topicLogReader.readTopics();

        // Step 2: Look up topic by name in topicMap
        Topic topic = topicMap.get(topicName);

        // Step 3: If topic is null, return error_code = 3
        if (topic == null) {
            System.out.println("Topic not found: " + topicName);
            return 3; // UNKNOWN_TOPIC_OR_PARTITION
        }

        // Step 4: Initialize flag partitionFound = false
        boolean partitionFound = false;

        // Step 5: For each partitionMetadata in topic.partitions
        List<List<Byte>> partitions = topic.getPartitions();

        if (partitions.isEmpty()) {
            System.out.println("Topic exists but has no partitions: " + topicName);
            return 3; // UNKNOWN_TOPIC_OR_PARTITION
        }

        for (List<Byte> partitionMetadata : partitions) {
            // Extract partition index from the partitionMetadata
            // Partition ID is stored as 4 bytes starting at position 2 (after 2 bytes of zeros)
            int extractedIndex = extractPartitionIndex(partitionMetadata);

            if (extractedIndex == -1) {
                System.err.println("Failed to extract partition index from metadata");
                continue;
            }

            // If extracted index equals requested partitionIndex, set partitionFound = true and break
            if (extractedIndex == partitionIndex) {
                partitionFound = true;
                break;
            }
        }

        // Step 6: If partitionFound is false, return error_code = 3
        if (!partitionFound) {
            System.out.println("Partition not found: " + partitionIndex + " for topic: " + topicName);
            return 3; // UNKNOWN_TOPIC_OR_PARTITION
        }

        // Step 7: Return error_code = 0 (success)
        return 0;
    }

    /**
     * Extract the partition index from partition metadata.
     * The partition ID is stored as 4 bytes (int32) starting at position 2.
     *
     * @param partitionMetadata The partition metadata as a list of bytes
     * @return The partition index, or -1 if extraction fails
     */
    private int extractPartitionIndex(List<Byte> partitionMetadata) {
        // Partition metadata structure:
        // - Bytes 0-1: zeros (2 bytes)
        // - Bytes 2-5: partition ID (4 bytes, int32, big-endian)
        // - Bytes 6+: other partition data

        if (partitionMetadata.size() < 6) {
            return -1; // Not enough data
        }

        try {
            // Extract 4 bytes starting at position 2 and convert to int (big-endian)
            int partitionIndex = 0;
            partitionIndex |= (partitionMetadata.get(2) & 0xFF) << 24;
            partitionIndex |= (partitionMetadata.get(3) & 0xFF) << 16;
            partitionIndex |= (partitionMetadata.get(4) & 0xFF) << 8;
            partitionIndex |= (partitionMetadata.get(5) & 0xFF);

            return partitionIndex;
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Error extracting partition index: " + e.getMessage());
            return -1;
        }
    }

    @Override
    public short getApiKey() {
        return 0; // Produce API key
    }

    /**
     * Handle empty or malformed requests with a minimal response.
     */
    private KafkaResponse handleEmptyRequest(int correlationId) {
        KafkaResponse response = new KafkaResponse(correlationId, true);
        response.addUnsignedVarInt(1); // empty topics array (length 0 -> 1)
        response.addBytes(0, 4); // throttle_time_ms = 0
        response.addUnsignedVarInt(0); // empty tagged_fields
        return response;
    }
}
