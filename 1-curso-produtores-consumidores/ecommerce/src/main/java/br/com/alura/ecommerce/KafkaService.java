package br.com.alura.ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService<T> {

    private final String groupId;
    private final ConsumerFunction<T> parse;
    private final KafkaConsumer<String, T> consumer;
    private final Class<T> type;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type) {
        this.groupId = groupId;
        this.parse = parse;
        this.type = type;
        this.consumer = new KafkaConsumer<>(getProperties(this.type, this.groupId));

        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topicPattern, ConsumerFunction<T> parse, Class<T> type) {
        this.groupId = groupId;
        this.parse = parse;
        this.type = type;
        this.consumer = new KafkaConsumer<>(getProperties(this.type, this.groupId));

        consumer.subscribe(topicPattern);
    }

    public KafkaService(String groupId, Pattern topicPattern, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this.groupId = groupId;
        this.parse = parse;
        this.type = type;
        this.consumer = new KafkaConsumer<>(getProperties(this.type, this.groupId));

        consumer.subscribe(topicPattern);
    }

    public void run() {
        while (true) {

            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registro(s)");

                for (var record : records) {
                    parse.consume(record);
                }
            }
        }

    }

    private Properties getProperties(Class<T> type, String groupId) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(GsonDeserializer.TYPE_CONFIG, type.getName());

        return properties;
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(GsonDeserializer.TYPE_CONFIG, type.getName());

        properties.putAll(properties);

        return properties;
    }    

}
