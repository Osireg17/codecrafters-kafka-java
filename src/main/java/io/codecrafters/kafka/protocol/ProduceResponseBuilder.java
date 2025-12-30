package io.codecrafters.kafka.protocol;

import java.nio.charset.StandardCharsets;

public class ProduceResponseBuilder {
    private final KafkaResponse response;

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
     * Build a Produce response v11 with UNKNOWN_TOPIC_OR_PARTITION error (error_code 3).
     *
     * @param topicName The topic name from the request
     * @param partitionIndex The partition index from the request
     */
    public void buildErrorResponse(String topicName, int partitionIndex) {
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

        // error_code (int16) = 3 (UNKNOWN_TOPIC_OR_PARTITION)
        response.addBytes(3, 2);

        // base_offset (int64) = -1
        response.addBytes((byte) 0xff, 8);

        // log_append_time_ms (int64) = -1
        response.addBytes((byte) 0xff, 8);

        // log_start_offset (int64) = -1
        response.addBytes((byte) 0xff, 8);

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
     * Build the final response.
     *
     * @return The KafkaResponse ready to send
     */
    public KafkaResponse build() {
        return response;
    }
}
