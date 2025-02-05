package dev.fastcampus.kafkatest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerController {

    private static final String TOPIC_NAME1 = "topic-test-1";
    private static final String TOPIC_NAME2 = "topic-test-2";

    @KafkaListener(topics = {TOPIC_NAME1, TOPIC_NAME2})
    public void listen(ConsumerRecords<String, String> records, Acknowledgment acknowledgment) {
        records.forEach(record -> log.info("consumer message size : {}", record.toString().length()));

        acknowledgment.acknowledge();
    }
}
