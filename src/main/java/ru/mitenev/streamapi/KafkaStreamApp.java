package ru.mitenev.streamapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mitenev.streamapi.model.Message;
import ru.mitenev.streamapi.serdes.MessageDeserializer;
import ru.mitenev.streamapi.serdes.MessageSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaStreamApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamApp.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String KAFKA_STREAM_APP_NAME = "kafka-streams-app";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String BLOCKED_USERS_TOPIC = "blocked-users";
    public static final String MESSAGES_TOPIC = "messages";
    public static final String FILTERED_MESSAGES_TOPIC = "filtered-messages";
    public static final String SYMBOL_FOR_REPLACE = "*";


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_STREAM_APP_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Path path = Path.of("offensive.txt");
        List<String> offensiveWords = Collections.emptyList();
        try {
            offensiveWords = Files.readAllLines(path);
        } catch (IOException e) {
            LOGGER.error("Error reading offensive words file");
        }

        StreamsBuilder builder = new StreamsBuilder();

        //Читаем из топика блокированных пользователей
        final KTable<String, String> blockedUsers = builder
                .table(BLOCKED_USERS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        //Читаем сообщения из топика messages
        Serdes.WrapperSerde<Message> messageSerdes = new Serdes.WrapperSerde<>(
                new MessageSerializer(objectMapper),
                new MessageDeserializer(objectMapper)
        );
        KStream<String, Message> messages = builder
                .stream(MESSAGES_TOPIC, Consumed.with(Serdes.String(), messageSerdes));
        List<String> finalOffensiveWords = offensiveWords;
        messages
                .peek((k, v) -> LOGGER.info("Message sent from [{}] to [{}]", v.getFrom(), v.getTo()))
                .map((sender, message) -> new KeyValue<>(message.getTo(), message))
                //Промежуточный топик нужен когджа изменяется ключ, в нем нужно явно указать сериализацию
                .repartition(Repartitioned.with(Serdes.String(), messageSerdes))
                        .leftJoin(blockedUsers, (message, blocked) -> {
            if(blocked == null || blocked.isEmpty()) {
                return message;
            }
            LOGGER.info("Проверка на блокировку отправителя получателем");
            List<String> usersNotAllowedToSend = List.of(blocked.split(","));
                    if (usersNotAllowedToSend.contains(message.getFrom())) {
                        LOGGER.info("User [{}] has been blocked by receiver", message.getFrom());
                        return null;
                    }
                    return message;
                        })
                .filter((k,v) -> v != null)
                .map((receiver, message) -> new KeyValue<>(message.getFrom(), message))
                .mapValues( value -> {
                    for (String badWord: finalOffensiveWords) {
                        String clearedText = value.getText().replaceAll(badWord, SYMBOL_FOR_REPLACE.repeat(badWord.length()));
                        value.setText(clearedText);
                    }
                    return value;
                })
                .to(FILTERED_MESSAGES_TOPIC);

        // создание и запуск потока
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // завершение работы потока при нажатии ctrl+c
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}