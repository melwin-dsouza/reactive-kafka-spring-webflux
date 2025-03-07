package com.rakbank.poc.kafka.config;

import com.rakbank.poc.kafka.consumer.ConsumerService;
import com.rakbank.poc.kafka.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.retry.Retry;

import java.time.Duration;

@Configuration
public class CcmClient {

    private WebClient webClient;


    Logger log = LoggerFactory.getLogger(CcmClient.class);

    @Autowired
    public CcmClient(WebClient webClient) {
        this.webClient = webClient;
    }


    public Mono<User> callCCM(User user){
        user.setName("flux-"+user.getName());
        log.info("Starting callCCM!- {}",user.toString());
        return webClient.post()
                .uri("http://localhost:8088/sendEmail")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(user)
                .exchangeToMono(r->{
                    if(r.statusCode().equals(HttpStatus.OK)){
                        return r.bodyToMono(User.class);
                    }else{
                        return r.createError();
                    }
                })
                ;
//                 .doOnNext(r->
//                                log.info(r.toString()))
//                 .doOnError(e->
//                                        log.error(e.toString()));
//                 .subscribe();
//                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(10)))



    }
}
