package com.rakbank.poc.kafka.consumer;


import com.rakbank.poc.kafka.config.CcmClient;
import com.rakbank.poc.kafka.data.User;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class ConsumerService {

    Logger log = LoggerFactory.getLogger(ConsumerService.class);

    private final ReactiveKafkaConsumerTemplate<String, User> reactiveKafkaConsumerTemplate;
    private final CcmClient ccmClient;
    public ConsumerService(ReactiveKafkaConsumerTemplate<String, User> reactiveKafkaConsumerTemplate, CcmClient ccmClient) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
        this.ccmClient = ccmClient;
    }

    @PostConstruct  // Start consumers automatically after bean creation
    public void startConsumers() {
        int consumerCount=3;
        Flux.range(1, consumerCount)  // Dynamically create `max-consumers` consumers
                .flatMap(consumerId -> consumeMessages())
                .subscribe();
    }



    public Mono<Void> consumeMessages() {
        // Consume messages reactively
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
//                .buffer(500)
                .bufferTimeout(500,Duration.ofSeconds(10))
                .doOnNext(recordBatch -> {
                    // Process each message
                    Flux.fromIterable(recordBatch)
                            .doOnNext(user->{
                     ccmClient.callCCM(user.value())
                             .subscribe(
                             result -> log.info("Received response: {}", result),
                        error -> log.error("Error occurred: {}, -{}",user.toString(), error.getMessage()));
                            })
                            .subscribe();
                    // Acknowledge the message
                })
                .onBackpressureBuffer(1000, // Buffer up to 1000 items in case of backpressure
                        (overflowItem) -> log.info("Backpressure overflow! Item dropped: {}",overflowItem.getFirst()))
                .doOnTerminate(() -> System.out.println("Kafka consumer stopped."))
//                .onBackpressureBuffer() // Optional: Apply backpressure handling
                .then();
    }

}
