package com.learn.kafka.producer.monitoring.controller;

import com.learn.kafka.producer.monitoring.utils.EmployeeEventGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);

    private final EmployeeEventGenerator generator;

    public ProducerController(EmployeeEventGenerator generator) {
        this.generator = generator;
    }

    @GetMapping("/start")
    public ResponseEntity<String> startProducer() {

        if (generator.isRunning()) return ResponseEntity.status(HttpStatus.CONFLICT).body("Producer already running");

        generator.start();

        logger.info("Producer started");

        return ResponseEntity.ok("Producer started successfully");
    }

    @GetMapping("/stop")
    public ResponseEntity<String> stopProducer() {

        if (!generator.isRunning()) return ResponseEntity.status(HttpStatus.CONFLICT).body("Producer already stopped");

        generator.stop();

        logger.info("Producer stopped");

        return ResponseEntity.ok("Producer stopped successfully");
    }

    @GetMapping("/status")
    public ResponseEntity<String> status() {
        return ResponseEntity.ok(generator.isRunning() ? "RUNNING" : "STOPPED");
    }
}