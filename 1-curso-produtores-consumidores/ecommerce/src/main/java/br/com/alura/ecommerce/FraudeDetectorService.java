package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorService {

    public static void main(String[] args) throws Exception {

        var fraudeService = new FraudeDetectorService();
        var service = new KafkaService<>(
                FraudeDetectorService.class.getSimpleName(), 
                "ECOMMERCE_NEW_ORDER", 
                fraudeService::parse,
                Order.class);

        service.run();

    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Processando New Order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("New Order Processed Successfully");
    }

}
