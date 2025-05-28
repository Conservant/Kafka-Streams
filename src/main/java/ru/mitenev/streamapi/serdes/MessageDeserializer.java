package ru.mitenev.streamapi.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mitenev.streamapi.model.Message;

import java.nio.charset.StandardCharsets;

public class MessageDeserializer implements Deserializer<Message> {
    private final ObjectMapper objectMapper;
    private final static Logger LOGGER = LoggerFactory.getLogger(MessageDeserializer.class);

    public MessageDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Message deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            String json = new String(bytes, StandardCharsets.UTF_8);
            return objectMapper.readValue(json, Message.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
