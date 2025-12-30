package io.codecrafters.kafka.protocol;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProduceResponseBuilder {
    private final KafkaResponse response;
    private boolean isMultiTopicMode = false;
    private int topicsCount = 0;
    private int currentTopicPartitionsCount = 0;

    /**
     * Create a Produce response builder with header v1.
     *
     * @param correlationId The correlation ID from the request
     */
    public ProduceResponseBuilder(int correlationId) {
        // Produce response v11 uses response header v1 (includes tagged_fields)
        this.response = new KafkaResponse(correlationId, true);
    }

    /**
     * Start building a multi-topic response.
     *
     * @param topicsCount The number of topics in the response
     */
    public void startMultiTopicResponse(int topicsCount) {
        this.isMultiTopicMode = true;
        this.topicsCount = topicsCount;

        // Step 1: Start response with responses array length
        response.addUnsignedVarInt(topicsCount + 1); // compact array: length + 1
    }

    /**
     * Start a topic in the response.
     *
     * @param topicName The name of the topic
     * @param partitionsCount The number of partitions for this topic
     */
    public void startTopic(String topicName, int partitionsCount) {
        this.currentTopicPartitionsCount = partitionsCount;

        // Step 2a: Write topic name
        byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
        response.addUnsignedVarInt(topicNameBytes.length + 1); // compact string
        response.addBytes(topicNameBytes);

        // Step 2b: Write partitions array length
        response.addUnsignedVarInt(partitionsCount + 1); // compact array: length + 1
    }

    /**
     * Add a partition result to the current topic.
     *
     * @param partitionIndex The partition index
     * @param errorCode The error code (0 for success, 3 for UNKNOWN_TOPIC_OR_PARTITION)
     */
    public void addPartitionResult(int partitionIndex, int errorCode) {
        // Step 2c: For each partition

        // partition index (int32)
        response.addBytes(partitionIndex, 4);

        // error_code (int16)
        response.addBytes(errorCode, 2);

        if (errorCode == 0) {
            // Success case
            // base_offset (int64) = 0
            response.addBytes(0, 8);

            // log_append_time_ms (int64) = -1
            response.addBytes((byte) 0xff, 8);

            // log_start_offset (int64) = 0
            response.addBytes(0, 8);
        } else {
            // Error case
            // base_offset (int64) = -1
            response.addBytes((byte) 0xff, 8);

            // log_append_time_ms (int64) = -1
            response.addBytes((byte) 0xff, 8);

            // log_start_offset (int64) = -1
            response.addBytes((byte) 0xff, 8);
        }

        // record_errors (compact array) = empty
        response.addUnsignedVarInt(1); // empty array (length 0 -> 1)

        // error_message (compact nullable string) = null
        response.addUnsignedVarInt(0); // null string

        // partition tagged_fields (varint) = empty
        response.addUnsignedVarInt(0);
    }

    /**
     * End the current topic (write topic tagged_fields).
     */
    public void endTopic() {
        // Step 2d: Write topic tagged_fields empty
        response.addUnsignedVarInt(0);
    }

    /**
     * End the multi-topic response (write throttle_time_ms and response tagged_fields).
     */
    public void endMultiTopicResponse() {
        // Step 3: Write throttle_time_ms = 0
        response.addBytes(0, 4);

        // Step 4: Write response tagged_fields empty
        response.addUnsignedVarInt(0);
    }

    /**
     * Build a Produce response v11 with a specific error code.
     *
     * @param topicName The topic name from the request
     * @param partitionIndex The partition index from the request
     * @param errorCode The error code (0 for success, 3 for UNKNOWN_TOPIC_OR_PARTITION)
     */
    public void buildResponse(String topicName, int partitionIndex, int errorCode) {
        // responses array (compact array) with 1 topic
        response.addUnsignedVarInt(1 + 1); // array length + 1 for compact encoding

        // topic name (compact string)
        byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
        response.addUnsignedVarInt(topicNameBytes.length + 1); // string length + 1
        response.addBytes(topicNameBytes);

        // partitions array (compact array) with 1 partition
        response.addUnsignedVarInt(1 + 1); // array length + 1 for compact encoding

        // partition index (int32)
        response.addBytes(partitionIndex, 4);

        // error_code (int16)
        response.addBytes(errorCode, 2);

        if (errorCode == 0) {
            // Success case
            // base_offset (int64) = 0
            response.addBytes(0, 8);

            // log_append_time_ms (int64) = -1
            response.addBytes((byte) 0xff, 8);

            // log_start_offset (int64) = 0
            response.addBytes(0, 8);
        } else {
            // Error case
            // base_offset (int64) = -1
            response.addBytes((byte) 0xff, 8);

            // log_append_time_ms (int64) = -1
            response.addBytes((byte) 0xff, 8);

            // log_start_offset (int64) = -1
            response.addBytes((byte) 0xff, 8);
        }

        // record_errors (compact array) = empty
        response.addUnsignedVarInt(1); // empty array (length 0 -> 1)

        // error_message (compact nullable string) = null
        response.addUnsignedVarInt(0); // null string

        // partition tagged_fields (varint) = empty
        response.addUnsignedVarInt(0);

        // topic tagged_fields (varint) = empty
        response.addUnsignedVarInt(0);

        // throttle_time_ms (int32) = 0
        response.addBytes(0, 4);

        // response body tagged_fields (varint) = empty
        response.addUnsignedVarInt(0);
    }

    /**
     * Build a Produce response v11 with UNKNOWN_TOPIC_OR_PARTITION error (error_code 3).
     *
     * @param topicName The topic name from the request
     * @param partitionIndex The partition index from the request
     */
    public void buildErrorResponse(String topicName, int partitionIndex) {
        buildResponse(topicName, partitionIndex, 3);
    }

    /**
     * Build a Produce response v11 with success (error_code 0).
     *
     * @param topicName The topic name from the request
     * @param partitionIndex The partition index from the request
     */
    public void buildSuccessResponse(String topicName, int partitionIndex) {
        buildResponse(topicName, partitionIndex, 0);
    }

    /**
     * Build the final response.
     *
     * @return The KafkaResponse ready to send
     */
    public KafkaResponse build() {
        return response;
    }
}
