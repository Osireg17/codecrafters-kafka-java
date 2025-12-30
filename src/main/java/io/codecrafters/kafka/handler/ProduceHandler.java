package io.codecrafters.kafka.handler;

import io.codecrafters.kafka.common.Topic;
import io.codecrafters.kafka.protocol.KafkaRequest;
import io.codecrafters.kafka.protocol.KafkaResponse;
import io.codecrafters.kafka.protocol.ProduceRequestParser;
import io.codecrafters.kafka.protocol.ProduceResponseBuilder;
import io.codecrafters.kafka.storage.PartitionLogReader;
import io.codecrafters.kafka.storage.PartitionLogWriter;
import io.codecrafters.kafka.storage.TopicLogReader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Handler for Produce requests (API key 0, version 11).
 * Validates topic and partition existence against __cluster_metadata and persists record batches to disk.
 * Supports multi-topic, multi-partition requests with fail-fast validation.
 */
public class ProduceHandler implements RequestHandler {

    private final TopicLogReader topicLogReader;
    private final PartitionLogReader partitionLogReader;
    private final PartitionLogWriter partitionLogWriter;
    private final String dataLogPath;

    /**
     * Result of topic and partition validation.
     */
    private static class ValidationResult {
        private final boolean valid;
        private final String errorMessage;
        private final String failedTopicName;
        private final int failedPartitionIndex;

        public ValidationResult(boolean valid) {
            this.valid = valid;
            this.errorMessage = null;
            this.failedTopicName = null;
            this.failedPartitionIndex = -1;
        }

        public ValidationResult(String errorMessage, String topicName, int partitionIndex) {
            this.valid = false;
            this.errorMessage = errorMessage;
            this.failedTopicName = topicName;
            this.failedPartitionIndex = partitionIndex;
        }

        public boolean isValid() {
            return valid;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getFailedTopicName() {
            return failedTopicName;
        }

        public int getFailedPartitionIndex() {
            return failedPartitionIndex;
        }
    }

    public ProduceHandler(String dataLogPath, String logFileName) {
        this.dataLogPath = dataLogPath;
        this.topicLogReader = new TopicLogReader(dataLogPath + "/__cluster_metadata-0", logFileName);
        this.partitionLogReader = new PartitionLogReader(
                dataLogPath,
                logFileName
        );
        this.partitionLogWriter = new PartitionLogWriter(dataLogPath);
    }

    @Override
    public KafkaResponse handle(KafkaRequest request) {
        ByteBuffer buffer = request.getRawBuffer();

        try {
            // Parse ALL topics and partitions from the Produce request
            List<ProduceRequestParser.ProduceTopic> produceTopics = ProduceRequestParser.parseAllTopics(buffer);

            if (produceTopics == null || produceTopics.isEmpty()) {
                System.err.println("Failed to parse Produce request - no topics or partitions found");
                return handleEmptyRequest(request.getCorrelationId());
            }

            System.out.println("Produce request with " + produceTopics.size() + " topic(s)");

            // Load topic metadata once for validation
            Map<String, Topic> topicMap = topicLogReader.readTopics();

            // Validate all topics and partitions (fail-fast on first invalid)
            ValidationResult validationResult = validateAllTopicsAndPartitions(produceTopics, topicMap);

            // If validation failed, return error response for the failed topic/partition
            if (!validationResult.isValid()) {
                System.out.println("Validation failed: " + validationResult.getErrorMessage());
                return buildErrorResponseForFailure(request.getCorrelationId(), produceTopics, validationResult);
            }

            // All topics/partitions are valid - persist record batches
            System.out.println("All topics and partitions validated successfully");
            appendAllRecordBatches(produceTopics);

            // Build success response for all topics and partitions
            return buildSuccessResponse(request.getCorrelationId(), produceTopics);

        } catch (Exception e) {
            System.err.println("Error handling Produce request: " + e.getMessage());
            e.printStackTrace();
            return handleEmptyRequest(request.getCorrelationId());
        }
    }

    /**
     * Validate all topics and partitions (fail-fast on first invalid).
     *
     * @param produceTopics List of topics with partitions to validate
     * @param topicMap Map of topic name to Topic metadata
     * @return ValidationResult indicating success or failure details
     */
    private ValidationResult validateAllTopicsAndPartitions(
            List<ProduceRequestParser.ProduceTopic> produceTopics,
            Map<String, Topic> topicMap) {

        // Step 1: topicMap already loaded by caller

        // Step 2: For each topic in request order
        for (ProduceRequestParser.ProduceTopic produceTopic : produceTopics) {
            String topicName = produceTopic.getTopicName();

            // Step 2a: Look up topic by name in topicMap
            Topic topic = topicMap.get(topicName);

            // Step 2b: If topic is null, return error_code = 3 for that topic
            if (topic == null) {
                String errorMsg = "Topic not found: " + topicName;
                System.out.println(errorMsg);
                // Return error for first partition (or -1 if no partitions)
                int firstPartitionIndex = produceTopic.getPartitions().isEmpty() ?
                    -1 : produceTopic.getPartitions().get(0).getPartitionIndex();
                return new ValidationResult(errorMsg, topicName, firstPartitionIndex);
            }

            // Step 2c: For each partition in topic
            for (ProduceRequestParser.ProducePartition partition : produceTopic.getPartitions()) {
                int partitionIndex = partition.getPartitionIndex();

                // Check partitionIndex exists in topic.partitions
                boolean partitionFound = isPartitionValid(topic, partitionIndex);

                // If not found, return error_code = 3 for that partition
                if (!partitionFound) {
                    String errorMsg = "Partition not found: " + partitionIndex + " for topic: " + topicName;
                    System.out.println(errorMsg);
                    return new ValidationResult(errorMsg, topicName, partitionIndex);
                }
            }
        }

        // Step 3: Return error_code = 0 (all valid)
        return new ValidationResult(true);
    }

    /**
     * Check if a partition exists in a topic.
     */
    private boolean isPartitionValid(Topic topic, int partitionIndex) {
        List<List<Byte>> partitions = topic.getPartitions();

        if (partitions.isEmpty()) {
            return false;
        }

        for (List<Byte> partitionMetadata : partitions) {
            int extractedIndex = extractPartitionIndex(partitionMetadata);
            if (extractedIndex == partitionIndex) {
                return true;
            }
        }

        return false;
    }

    /**
     * Append all record batches to their respective partition log files.
     *
     * @param produceTopics List of topics with partitions and record batches
     */
    private void appendAllRecordBatches(List<ProduceRequestParser.ProduceTopic> produceTopics) {
        // Step 1: For each topic
        for (ProduceRequestParser.ProduceTopic produceTopic : produceTopics) {
            String topicName = produceTopic.getTopicName();

            // Step 1a: For each partition
            for (ProduceRequestParser.ProducePartition partition : produceTopic.getPartitions()) {
                int partitionIndex = partition.getPartitionIndex();
                byte[] recordBatchBytes = partition.getRecordBatchBytes();

                // Append recordBatchBytes if not empty
                if (recordBatchBytes != null && recordBatchBytes.length > 0) {
                    try {
                        System.out.println("Appending " + recordBatchBytes.length +
                                         " bytes to topic: " + topicName +
                                         ", partition: " + partitionIndex);
                        partitionLogWriter.appendRecordBatch(topicName, partitionIndex, recordBatchBytes);
                        System.out.println("Record batch successfully persisted");
                    } catch (Exception e) {
                        System.err.println("Error writing record batch to disk: " + e.getMessage());
                        e.printStackTrace();
                        // Continue with other partitions
                    }
                } else {
                    System.out.println("Skipping empty record batch for topic: " + topicName +
                                     ", partition: " + partitionIndex);
                }
            }
        }
    }

    /**
     * Build success response for all topics and partitions.
     */
    private KafkaResponse buildSuccessResponse(
            int correlationId,
            List<ProduceRequestParser.ProduceTopic> produceTopics) {

        ProduceResponseBuilder builder = new ProduceResponseBuilder(correlationId);

        // Start multi-topic response
        builder.startMultiTopicResponse(produceTopics.size());

        // For each topic
        for (ProduceRequestParser.ProduceTopic produceTopic : produceTopics) {
            String topicName = produceTopic.getTopicName();
            List<ProduceRequestParser.ProducePartition> partitions = produceTopic.getPartitions();

            // Start topic
            builder.startTopic(topicName, partitions.size());

            // For each partition, add success result
            for (ProduceRequestParser.ProducePartition partition : partitions) {
                builder.addPartitionResult(partition.getPartitionIndex(), 0); // error_code = 0
            }

            // End topic
            builder.endTopic();
        }

        // End response
        builder.endMultiTopicResponse();

        return builder.build();
    }

    /**
     * Build error response when validation fails.
     * Returns error for the failed partition and success for all previous ones.
     */
    private KafkaResponse buildErrorResponseForFailure(
            int correlationId,
            List<ProduceRequestParser.ProduceTopic> produceTopics,
            ValidationResult validationResult) {

        ProduceResponseBuilder builder = new ProduceResponseBuilder(correlationId);

        // Start multi-topic response
        builder.startMultiTopicResponse(produceTopics.size());

        String failedTopicName = validationResult.getFailedTopicName();
        int failedPartitionIndex = validationResult.getFailedPartitionIndex();
        boolean failureEncountered = false;

        // For each topic
        for (ProduceRequestParser.ProduceTopic produceTopic : produceTopics) {
            String topicName = produceTopic.getTopicName();
            List<ProduceRequestParser.ProducePartition> partitions = produceTopic.getPartitions();

            // Start topic
            builder.startTopic(topicName, partitions.size());

            // For each partition
            for (ProduceRequestParser.ProducePartition partition : partitions) {
                int partitionIndex = partition.getPartitionIndex();

                // Check if this is the failed partition
                if (topicName.equals(failedTopicName) && partitionIndex == failedPartitionIndex) {
                    builder.addPartitionResult(partitionIndex, 3); // error_code = 3
                    failureEncountered = true;
                } else if (failureEncountered) {
                    // After failure, return error for all remaining partitions
                    builder.addPartitionResult(partitionIndex, 3); // error_code = 3
                } else {
                    // Before failure, could return success (but typically fail-fast means error for all)
                    builder.addPartitionResult(partitionIndex, 3); // error_code = 3 for consistency
                }
            }

            // End topic
            builder.endTopic();
        }

        // End response
        builder.endMultiTopicResponse();

        return builder.build();
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
