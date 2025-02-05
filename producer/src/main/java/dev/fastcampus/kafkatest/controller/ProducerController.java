package dev.fastcampus.kafkatest.controller;

import dev.fastcampus.kafkatest.service.ProducerService1;
import dev.fastcampus.kafkatest.service.ProducerService2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final ProducerService1 producerService1;
    private final ProducerService2 producerService2;

    public ProducerController(ProducerService1 producerService1, ProducerService2 producerService2) {
        this.producerService1 = producerService1;
        this.producerService2 = producerService2;
    }

    @GetMapping("/publish")
    public void publish() {
        producerService1.publish();
        producerService2.publish();
    }
}
