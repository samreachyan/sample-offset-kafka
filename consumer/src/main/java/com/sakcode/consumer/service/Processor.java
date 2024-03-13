package com.sakcode.consumer.service;

import com.sakcode.common.Order;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class Processor {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public void process(Order order, Acknowledgment acknowledgment) {
        logger.info("Processing: {}", order.getId());
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finished: {}", order.getId());
        acknowledgment.acknowledge();
    }
}
