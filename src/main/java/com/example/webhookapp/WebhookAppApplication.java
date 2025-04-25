package com.example.webhookapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootApplication
public class WebhookAppApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebhookAppApplication.class, args);
    }

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .baseUrl("https://bfhldevapigw.healthrx.co.in")
                .build();
    }
} 