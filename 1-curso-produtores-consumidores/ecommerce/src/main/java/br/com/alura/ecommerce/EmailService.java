package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) throws Exception {
        var emailService = new EmailService();
        var service = new KafkaService<>(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL", 
                emailService::parse,
                String.class);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Sending email...");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Email enviado");
    }

}
