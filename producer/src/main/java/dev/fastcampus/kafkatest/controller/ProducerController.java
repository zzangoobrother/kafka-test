package dev.fastcampus.kafkatest.controller;

import dev.fastcampus.kafkatest.service.ProducerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping("/publish")
    public void publish() {
        producerService.publish();
    }
}
