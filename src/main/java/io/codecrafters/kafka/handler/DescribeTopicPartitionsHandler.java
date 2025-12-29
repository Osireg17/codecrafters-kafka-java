package io.codecrafters.kafka.handler;

import io.codecrafters.kafka.common.Topic;
import io.codecrafters.kafka.protocol.ByteUtils;
import io.codecrafters.kafka.protocol.KafkaRequest;
import io.codecrafters.kafka.protocol.KafkaResponse;
import io.codecrafters.kafka.storage.TopicLogReader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DescribeTopicPartitionsHandler implements RequestHandler {

    private final TopicLogReader topicLogReader;

    public DescribeTopicPartitionsHandler(String dataLogPath, String logFileName) {
        this.topicLogReader = new TopicLogReader(dataLogPath, logFileName);
    }

    @Override
    public short getApiKey() {
        return 75;
    }

    @Override
    public KafkaResponse handle(KafkaRequest request) {
        KafkaResponse response = new KafkaResponse(request.getCorrelationId());
        ByteBuffer buffer = request.getRawBuffer();

        Map<String, Topic> topicMap = topicLogReader.readTopics();
        System.out.println(topicMap);

        buffer.position(12);
        buffer.position(15 + buffer.getShort());
        int arrayLen = buffer.get();

        response.addBytes((byte) 0, 5);
        response.addBytes(arrayLen, 1);

        for (int i = 0, length = arrayLen - 1; i < length; i++) {
            int topicNameLen = buffer.get();
            byte[] topicName = new byte[topicNameLen - 1];
            buffer.get(topicName, 0, topicName.length);

            Topic topic = topicMap.get(new String(topicName));

            response.addBytes(Objects.isNull(topic) ? 3 : 0, 2);
            response.addBytes(topicNameLen, 1);
            response.addBytes(topicName);

            if (!Objects.isNull(topic)) {
                response.addBytes(topic.getTopicId());
            } else {
                response.addBytes((byte) 0, 16);
            }

            response.addByte((byte) 0);

            if (Objects.isNull(topic)) {
                response.addByte((byte) 1);
            } else {
                response.addByte((byte) (topic.getPartitions().size() + 1));
                topic.getPartitions().forEach(response::addBytes);
            }

            response.addBytes(0x0df8, 4);
            response.addByte((byte) 0);
            buffer.position(buffer.position() + 1);
        }

        response.addBytes(0xff, 1);
        response.addByte((byte) 0);

        return response;
    }
}
