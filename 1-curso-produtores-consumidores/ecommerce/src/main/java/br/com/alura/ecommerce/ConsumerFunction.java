package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ConsumerFunction<T> {

    void consume(ConsumerRecord<String, T> record);

}
