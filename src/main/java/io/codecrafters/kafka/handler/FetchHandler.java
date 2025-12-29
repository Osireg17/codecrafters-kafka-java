package io.codecrafters.kafka.handler;

import io.codecrafters.kafka.protocol.KafkaRequest;
import io.codecrafters.kafka.protocol.KafkaResponse;

public class FetchHandler implements RequestHandler {

    @Override
    public short getApiKey() {
        return 1;
    }

    @Override
    public KafkaResponse handle(KafkaRequest request) {
        KafkaResponse response = new KafkaResponse(request.getCorrelationId(), true);

        // throttle_time_ms (INT32) - 4 bytes
        response.addBytes((byte) 0, 4);

        // error_code (INT16) - 2 bytes
        response.addBytes((byte) 0, 2);

        // session_id (INT32) - 4 bytes
        response.addBytes((byte) 0, 4);

        // responses array length (COMPACT_ARRAY)
        // 0 elements = length 1 (0 + 1 for compact encoding)
        response.addByte((byte) 1);

        // _tagged_fields (empty)
        response.addByte((byte) 0);

        return response;
    }
}