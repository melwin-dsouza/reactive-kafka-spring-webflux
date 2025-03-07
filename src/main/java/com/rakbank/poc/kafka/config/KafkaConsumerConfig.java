package com.rakbank.poc.kafka.config;

import com.rakbank.poc.kafka.data.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {


    @Bean
    public KafkaReceiver<String, User> kafkaReceiver() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "reactor-consumer-group");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.rakbank.poc.kafka.data");
        ReceiverOptions<String, User> receiverOptions = ReceiverOptions.create(config);
        receiverOptions = receiverOptions.subscription(Collections.singletonList("kafka-mvd"));

        return KafkaReceiver.create(receiverOptions);
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, User> reactiveKafkaConsumerTemplate() {
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiver());
    }


    @Bean
    public WebClient webClient(){
        ConnectionProvider connectionProvider=ConnectionProvider.builder("custom")
                .maxConnections(5000)
                .maxIdleTime(Duration.ofSeconds(20))
                .maxLifeTime(Duration.ofSeconds(60))
                .pendingAcquireTimeout(Duration.ofSeconds(60))
                .evictInBackground(Duration.ofSeconds(120))
                .pendingAcquireMaxCount(5000)
                .build();
        HttpClient httpClient=HttpClient.create(connectionProvider);

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}