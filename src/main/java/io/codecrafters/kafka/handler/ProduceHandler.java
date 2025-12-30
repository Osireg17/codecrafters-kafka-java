package io.codecrafters.kafka.handler;

import io.codecrafters.kafka.protocol.KafkaRequest;
import io.codecrafters.kafka.protocol.KafkaResponse;
import io.codecrafters.kafka.protocol.ProduceRequestParser;
import io.codecrafters.kafka.protocol.ProduceResponseBuilder;

import java.nio.ByteBuffer;

/**
 * Handler for Produce requests (API key 0, version 11).
 * Parses the first topic name and partition index, then returns UNKNOWN_TOPIC_OR_PARTITION error.
 */
public class ProduceHandler implements RequestHandler {

    @Override
    public KafkaResponse handle(KafkaRequest request) {
        ByteBuffer buffer = request.getRawBuffer();

        try {
            // Parse the Produce request to extract first topic name and partition index
            ProduceRequestParser.ProduceData produceData = ProduceRequestParser.parse(buffer);

            if (produceData == null) {
                System.err.println("Failed to parse Produce request - no topics or partitions found");
                return handleEmptyRequest(request.getCorrelationId());
            }

            System.out.println("Produce request for topic: " + produceData.getTopicName() +
                             ", partition: " + produceData.getPartitionIndex());

            // Build error response with UNKNOWN_TOPIC_OR_PARTITION (error code 3)
            ProduceResponseBuilder builder = new ProduceResponseBuilder(request.getCorrelationId());
            builder.buildErrorResponse(produceData.getTopicName(), produceData.getPartitionIndex());

            return builder.build();

        } catch (Exception e) {
            System.err.println("Error parsing Produce request: " + e.getMessage());
            e.printStackTrace();
            return handleEmptyRequest(request.getCorrelationId());
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
