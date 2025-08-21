package com.example.backend.controller;

import com.example.backend.dto.*;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;

@RestController
@RequestMapping("/api")
public class QueryController {

    private final WebClient webClient;
    // Java database connectivity
    private final JdbcTemplate jdbc;

    public QueryController(WebClient.Builder webClientBuilder, JdbcTemplate jdbc) {
        // PY_SERVICE_BASEURL should be like http://python-llm:8000
        this.webClient = webClientBuilder
                .baseUrl(System.getenv().getOrDefault("PY_SERVICE_BASEURL", "http://python-llm:8000")).build();
        this.jdbc = jdbc;
    }

    @PostMapping(path = "/run", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RunResponse> runEndToEnd(@RequestBody RunRequest req) {

        // 1) ask python to generate SQL
        Map<String, Object> payload = Map.of(
                "question", req.question(), // if you used a POJO, use req.getQuestion()
                "backend", req.backend());

        // Sends the payload creatred above to the Pyhton service that must be running
        // at the base URL and whatever th name of the sql generating endpoint is.
        // rest is handled by python in terms of what is used and how it is used
        GenSqlResponse gen = webClient.post()
                .uri("/v1/generate-sql")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .retrieve()
                .bodyToMono(GenSqlResponse.class)
                .block(); // ok in this small demo

        String sql = gen.sql();
        // parsing and making an Array List out of the warnings returned
        List<String> warnings = new ArrayList<>(Optional.ofNullable(gen.warnings()).orElse(List.of()));

        // 2) execute SQL (read-only)
        // parsing the SQL querry results into usable and readable format
        List<Map<String, Object>> rowsAsMaps = jdbc.queryForList(sql);
        // <column name, the value for that column in that row>

        // turn into rows/headers for the UI
        List<String> headers = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();

        if (!rowsAsMaps.isEmpty()) {
            // save the headers by getting the names of the keys in the first element
            headers.addAll(rowsAsMaps.get(0).keySet());
            // separates the values form the keys and only contains the results in the cells of the db
            for (Map<String, Object> r : rowsAsMaps) {
                List<Object> one = new ArrayList<>();
                for (String h : headers) {
                    one.add(r.get(h));
                }
                rows.add(one);
            }
        }

        // 3) mock LLM final answer (so your UI shows something)
        // has to be changed when a real llm is used
        String answer = String.format("Here are the last %d missions by launch date.", rows.size());

        // returns an object with all of the elements that the process is 
        // supposed to yield in a single class object 
        return ResponseEntity.ok(new RunResponse(sql, headers, rows, warnings, answer));
    }
}
