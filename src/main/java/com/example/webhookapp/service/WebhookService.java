package com.example.webhookapp.service;

import com.example.webhookapp.model.GenerateWebhookRequest;
import com.example.webhookapp.model.GenerateWebhookResponse;
import com.example.webhookapp.model.GenerateWebhookResponse.User;
import com.example.webhookapp.model.GenerateWebhookResponse.UsersData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebhookService {

    private final WebClient webClient;

    @jakarta.annotation.PostConstruct
    public void init() {
        processWebhook();
    }

    public void processWebhook() {
        generateWebhookToken()
            .flatMap(response -> {
                String accessToken = response.getAccessToken();
                log.info("Generated access token: {}", accessToken);
     
                // Step 2: Solve the problem using data from the response
                List<List<Integer>> solution = solveProblem(response.getData());
                log.info("Solution: {}", solution);
                
                // Step 3: Send solution to webhook with retries
                return sendSolutionToWebhook(accessToken, solution, response.getWebhook())
                    .retryWhen(Retry.backoff(4, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(10))
                        .filter(throwable -> throwable instanceof WebClientResponseException.TooManyRequests));
            })
            .block();
    }

    @Retryable(
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public Mono<GenerateWebhookResponse> generateWebhookToken() {
        GenerateWebhookRequest request = GenerateWebhookRequest.builder()
            .name("Prashant Singh")
            .regNo("RA22110033010074")
            .email("ps5922@srmist.edu.in")
            .build();

        return webClient.post()
            .uri("/hiring/generateWebhook")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(GenerateWebhookResponse.class)
            .doOnError(error -> log.error("Error generating webhook token: {}", error.getMessage()))
            .doOnSuccess(response -> log.info("Successfully generated webhook token"));
    }

    public List<List<Integer>> solveProblem(UsersData usersData) {
        List<User> users = usersData.getUsers();
        int findId = usersData.getFindId();
        int n = usersData.getN();

        // Create a map for quick user lookup
        Map<Integer, User> userMap = new HashMap<>();
        for (User user : users) {
            userMap.put(user.getId(), user);
        }

        // Initialize BFS
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> queue = new LinkedList<>();
        Map<Integer, Integer> levels = new HashMap<>();
        
        // Start with the given user ID
        queue.add(findId);
        levels.put(findId, 0);
        visited.add(findId);

        // Perform BFS
        while (!queue.isEmpty()) {
            int current = queue.poll();
            int currentLevel = levels.get(current);
            
            // If we've reached the desired level, continue to next node
            if (currentLevel >= n) {
                continue;
            }

            User currentUser = userMap.get(current);
            if (currentUser == null) {
                continue;
            }

            // Process all followers
            for (int followerId : currentUser.getFollows()) {
                if (!visited.contains(followerId)) {
                    visited.add(followerId);
                    levels.put(followerId, currentLevel + 1);
                    queue.add(followerId);
                }
            }
        }

        // Collect all users at exactly n levels
        List<Integer> result = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : levels.entrySet()) {
            if (entry.getValue() == n) {
                result.add(entry.getKey());
            }
        }

        // Sort the result
        Collections.sort(result);

        // Convert to the required format
        List<List<Integer>> finalResult = new ArrayList<>();
        finalResult.add(result);
        
        return finalResult;
    }

    private Mono<Void> sendSolutionToWebhook(String accessToken, List<List<Integer>> solution, String webhookUrl) {
        WebhookSolutionRequest request = WebhookSolutionRequest.builder()
            .regNo("RA22110033010074")
            .outcome(solution)
            .build();

        return webClient.post()
            .uri(webhookUrl)
            .header(HttpHeaders.AUTHORIZATION, accessToken)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(Void.class)
            .doOnError(error -> log.error("Error sending solution to webhook: {}", error.getMessage()))
            .doOnSuccess(v -> log.info("Successfully sent solution to webhook"));
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    private static class WebhookSolutionRequest {
        private String regNo;
        private List<List<Integer>> outcome;
    }
} 