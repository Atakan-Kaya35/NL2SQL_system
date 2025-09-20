package com.example.backend.controller;

import com.example.backend.dto.*;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@RestController
@RequestMapping("/api")
public class QueryController {

    private static final Logger log = LoggerFactory.getLogger(QueryController.class);
    public static final String NL2SQL_SYSTEM_PROMPT = """
    redacted
            """;

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
                "schema_ddl", NL2SQL_SYSTEM_PROMPT);
        log.info("!!!!!Payload to LLM: {}", payload);

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

        log.info("!!!!!LLM response: response, {}, dialect {}, warnings{}", gen.response(), gen.dialect(), gen.warnings());
        String sql = gen.response();
        // parsing and making an Array List out of the warnings returned
        List<String> warnings = new ArrayList<>(Optional.ofNullable(gen.warnings()).orElse(List.of()));

        // 2) execute SQL (read-only)
        // parsing the SQL querry results into usable and readable format
        List<Map<String, Object>> rowsAsMaps = jdbc.queryForList(sql);

        String sqlFindings = convertRowsToString(rowsAsMaps);
        payload = Map.of(
                "question", req.question(), // if you used a POJO, use req.getQuestion()
                "context", sqlFindings);

        GenIntuitionResponse gen1 = webClient.post()
            .uri("/v1/generate-intuition")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(payload)
            .retrieve()
            .bodyToMono(GenIntuitionResponse.class)
            .block(); // ok in this small demo

        log.info("!!!!!Intuition response: {}", gen1.answer());
        
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
        String answer = Optional.ofNullable(gen1)
                .map(GenIntuitionResponse::answer) // if it's a record; use getAnswer() if it's a POJO
                .filter(a -> !a.isBlank())
                .orElse("No model answer was produced.");

        // returns an object with all of the elements that the process is 
        // supposed to yield in a single class object 
        return ResponseEntity.ok(new RunResponse(sql, headers, rows, warnings, answer));
    }

    public String convertRowsToString(List<Map<String, Object>> rowsAsMaps) {
        if (rowsAsMaps == null || rowsAsMaps.isEmpty()) {
            return "No data found for the query.";
        }

        StringBuilder result = new StringBuilder();

        // Start with a description of the data
        result.append("The following data was retrieved from the database:\n\n");

        // Iterate through each row (Map) in the list
        for (int i = 0; i < rowsAsMaps.size(); i++) {
            Map<String, Object> row = rowsAsMaps.get(i);

            // Append a header for each row to make it distinct
            result.append("--- Row ").append(i + 1).append(" ---\n");

            // Iterate through each column (key-value pair) in the current row
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String columnName = entry.getKey();
                Object value = entry.getValue();

                // Format the column name and value for readability
                result.append(columnName)
                        .append(": ")
                        .append(value != null ? value.toString() : "null")
                        .append("\n");
            }
            result.append("\n"); // Add a blank line for separation
        }

        return result.toString();
    }
}
