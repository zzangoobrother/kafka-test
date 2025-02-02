package dev.fastcampus.kafkatest.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class ProducerService {

    private static final String TOPIC_NAME = "topic-test";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String message) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 1024 * 1024 * 1.5) {
            sb.append("1");
        }

        log.info("message size : {}", sb.length());
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC_NAME, sb.toString());
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("send : {}, offset : {}", message, result.getRecordMetadata().offset());
            } else {
                log.info("failed : {}, error message : {}", message, ex.getMessage());
            }
        });
    }
}
