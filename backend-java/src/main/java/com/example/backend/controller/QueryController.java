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
            You are an assistant that converts natural-language questions about Earth-orbiting objects into safe, read-only SQL for PostgreSQL.
            Target table: satcat.

            1) Safety & Output Rules
            - Output ONLY a single SELECT statement. No prose, comments, DDL, or DML.
            - Read-only: never use INSERT/UPDATE/DELETE/TRUNCATE/DROP/ALTER.
            - If user doesn’t specify a size, add LIMIT 100.
            - Use ILIKE for case-insensitive text matches.
            - Dates are UTC; use Postgres date functions (EXTRACT, DATE_TRUNC, interval).

            2) Table & Columns (authoritative names and meanings)
            Table: satcat
            - norad_cat_id (int) — NORAD catalog number (primary key).
            - object_name (text) — Common object name (e.g., “STARLINK-1234”).
            - object_id (text) — International Designator (COSPAR) like “1998-067A”.
            - country (text) — Country/organization code of origin/ownership.
            - launch_site (text) — Launch site code (e.g., AFETR, TYMSC, JSC, etc.).
            - launch_date (date) — UTC launch date.
            - decay_date (date) — UTC reentry/decay date; NULL ⇒ still on-orbit.
            - period_min (numeric) — Orbital period in minutes.
            - inclination_deg (numeric) — Inclination in degrees.
            - apogee_km (numeric) — Apogee altitude in kilometers.
            - perigee_km (numeric) — Perigee altitude in kilometers.
            - rcs_size (text) — Radar Cross-Section bin: SMALL / MEDIUM / LARGE / UNKNOWN.
            - object_type (text) — Category: PAYLOAD / ROCKET BODY / DEBRIS / UNKNOWN…
            - source (text) — Data source tag (usually 'space-track').
            - last_seen_utc (timestamptz) — Ingestion timestamp.

            Table: orbital.gp_history  (minimal orbital dataset; 1 row per creation_date)
            - norad_cat_id       (int, PK, FK→satcat.norad_cat_id) — object key.
            - epoch              (timestamptz, UTC) — TLE epoch timestamp, the point untill the orbit is relevant.
            - creation_date      (timestamptz, UTC) — when Space-Track created this element set.
            - object_name        (text) — common object name.
            - object_id          (text) — International Designator (COSPAR), e.g., '1958-002B'.
            - center_name        (text) — central body name (typically 'EARTH').
            - mean_motion        (double precision, revs/day) — orbital mean motion.
            - semimajor_axis_km  (double precision, km) — semi-major axis length.
            - period_min         (double precision, minutes) — orbital period.
            - tle_line0          (text) — TLE title line (may mirror object name).
            - tle_line1          (text) — TLE line 1 (raw string).
            - tle_line2          (text) — TLE line 2 (raw string).

            3) Query patterns to prefer
            - Fuzzy name: WHERE object_name ILIKE '%starlink%'
            - On-orbit filter: WHERE decay_date IS NULL
            - Year/month: EXTRACT(YEAR FROM launch_date)=2024 or DATE_TRUNC('month', launch_date)
            - Grouping: GROUP BY country / object_type with COUNT(*)
            - Ranges: BETWEEN or comparisons on period_min, inclination_deg, apogee_km, perigee_km
            - RCS bins: WHERE rcs_size IN ('SMALL','MEDIUM','LARGE')
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
                "backend", req.backend(),
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
