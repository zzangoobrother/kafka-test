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

    public void publish() {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 1024 * 1024 * 0.5) {
            sb.append("1");
        }

        log.info("message size : {}", sb.length());

        CompletableFuture.allOf(
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString()),
                create(sb.toString())
        ).join();


    }

    private CompletableFuture create(String message) {
        return CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message)),
                CompletableFuture.runAsync(() -> send(message))
        );
    }

    private void send(String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC_NAME, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("send size : {}, offset : {}", message.length(), result.getRecordMetadata().offset());
            } else {
                log.info("failed size : {}, error message : {}", message.length(), ex.getMessage());
            }
        });
    }
}
