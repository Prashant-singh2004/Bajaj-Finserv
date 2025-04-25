package com.example.webhookapp.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GenerateWebhookResponse {
    private String webhook;
    private String accessToken;
    private UsersData data;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UsersData {
        private List<User> users;
        private int findId;
        private int n;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User {
        private int id;
        private List<Integer> follows;
    }
} 