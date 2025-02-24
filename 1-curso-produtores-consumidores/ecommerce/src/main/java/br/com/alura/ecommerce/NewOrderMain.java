package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;

public class NewOrderMain {

    public static void main(String[] args) throws Exception {

        var orderDispatcher = new KafkaDispatcher<Order>();
        var emailDispatcher = new KafkaDispatcher<String>();

        for (int i = 0; i < 10; i++) {
            var userId = UUID.randomUUID().toString();
            var orderId = UUID.randomUUID().toString();
            var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

            var newOrder = new Order(userId, orderId, amount);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, newOrder);
            
            var email = "Thank you for your new order! We are processing your new order!";
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);            
        }

    }

}
