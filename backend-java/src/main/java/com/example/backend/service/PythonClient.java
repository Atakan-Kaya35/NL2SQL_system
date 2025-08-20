package com.example.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Service
public class PythonClient {
    private final WebClient webClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public PythonClient(@Value("${app.pythonServiceBaseUrl}") String baseUrl) {
        this.webClient = WebClient.builder().baseUrl(baseUrl).build();
    }

    public Mono<GenerateSQLResult> generateSql(String question, String ddl, String backend) {
        String payload = String.format("{\"question\":\"%s\",\"schema_ddl\":%s%s}",
                escapeJson(question),
                ddl == null ? "null" : "\"" + escapeJson(ddl) + "\"",
                backend == null ? "" : ",\"backend\":\"" + escapeJson(backend) + "\"");

        return webClient.post()
                .uri("/v1/generate-sql")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .retrieve()
                .bodyToMono(String.class)
                .map(body -> {
                    try {
                        JsonNode node = mapper.readTree(body);
                        String sql = node.get("sql").asText();
                        String warnings = node.has("warnings") ? node.get("warnings").toString() : "[]";
                        return new GenerateSQLResult(sql, warnings);
                    } catch (Exception e) {
                        throw new RuntimeException("Invalid response from Python service: " + body, e);
                    }
                });
    }

    public Mono<String> draftAnswer(String question, String sql, List<Map<String, Object>> rows, String backend) {
        try {
            var root = mapper.createObjectNode();
            root.put("question", question);
            root.put("sql", sql);
            root.put("backend", backend == null ? "" : backend);
            root.set("rows", mapper.valueToTree(rows));
            String payload = mapper.writeValueAsString(root);

            return webClient.post()
                    .uri("/v1/draft-answer")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(payload)
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(body -> {
                        try {
                            JsonNode node = mapper.readTree(body);
                            return node.get("answer").asText();
                        } catch (Exception e) {
                            throw new RuntimeException("Invalid response from Python service: " + body, e);
                        }
                    });
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public record GenerateSQLResult(String sql, String warningsJson) {
    }
}
