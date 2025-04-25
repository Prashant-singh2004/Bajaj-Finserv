package com.example.webhookapp;

import com.example.webhookapp.service.WebhookService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class WebhookInitializer {

    private final WebhookService webhookService;

    @PostConstruct
    public void initialize() {
        webhookService.processWebhook();
    }
} 