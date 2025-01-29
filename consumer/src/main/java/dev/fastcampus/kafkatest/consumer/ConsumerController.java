package dev.fastcampus.kafkatest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerController {

    private static final String TOPIC_NAME = "topic-test";

    @KafkaListener(topics = TOPIC_NAME)
    public void listen(String message) {
        log.info("consumer message : {}", message);
    }
}
