package br.com.alura.ecommerce;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class LogService {

    public static void main(String[] args) throws Exception {

        var logService = new LogService();
        var kafkaService = new KafkaService<>(
                LogService.class.getSimpleName(), 
                Pattern.compile("ECOMMERCE_.*"), 
                logService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, String.class.getName()));

        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("--------------------------------------------");
        System.out.println("LOG");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }

}
