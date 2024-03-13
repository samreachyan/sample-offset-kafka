package com.sakcode.producer.rest;

import com.sakcode.common.Order;
import com.sakcode.producer.dto.InputParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
public class TransactionsController {
    private static final Logger logger = LoggerFactory.getLogger(TransactionsController.class);


    long id = 1;
    long groupId = 1;

    @Autowired
    private KafkaTemplate<Long, Order> kafkaTemplate;


    @PostMapping("/transactions")
    public void generateAndSendMessages(@RequestBody InputParameter inputParameters) {
        for (long i = 0; i < inputParameters.getNumberOfMessage(); i++) {
            Order o = new Order(id++, i + 1, i + 2, 1000, "NEW", groupId);
            CompletableFuture<SendResult<Long, Order>> result = kafkaTemplate.send("transactions-async", o.getId(), o);
            result.whenComplete((sr, ex) -> logger.info("Sent({}): {}", sr.getProducerRecord().key(), sr.getProducerRecord().value()));
        }
        groupId++;
        logger.info("{} already sent to Ordered", inputParameters.getNumberOfMessage());
    }
}
