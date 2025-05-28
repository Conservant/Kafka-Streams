package ru.mitenev.streamapi.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mitenev.streamapi.model.Message;

public class MessageSerializer implements Serializer<Message> {
    private final ObjectMapper objectMapper;
    private final static Logger LOGGER = LoggerFactory.getLogger(MessageSerializer.class);

    public MessageSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            LOGGER.error("Ошибка сериализации данных");
            return null;
        }
    }
}
