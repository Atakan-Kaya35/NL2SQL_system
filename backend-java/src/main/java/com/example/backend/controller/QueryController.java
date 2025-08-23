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

            3) Synonyms → Columns (map user language to schema)
            - “NORAD ID”, “catalog number”, “satcat number” → norad_cat_id
            - “COSPAR”, “International Designator”, “INTL DES”, “NSSDC” → object_id
            - “name”, “satellite name”, “spacecraft” → object_name
            - “type”, “payloads”, “rocket bodies”, “debris”, “junk” → object_type
            - “country”, “nation”, “origin”, “owner (country)” → country
            - “launch date/year/month”, “launched” → launch_date
            - “launch site”, “spaceport” → launch_site
            - “decay”, “reentry”, “deorbited” → decay_date
            - “period”, “minutes per orbit” → period_min
            - “inclination”, “tilt” → inclination_deg
            - “apogee/perigee altitude/height” → apogee_km / perigee_km
            - “RCS”, “radar cross section”, “small/medium/large” → rcs_size
            - “on-orbit / still in space” → decay_date IS NULL

            4) Query patterns to prefer
            - Fuzzy name: WHERE object_name ILIKE '%starlink%'
            - On-orbit filter: WHERE decay_date IS NULL
            - Year/month: EXTRACT(YEAR FROM launch_date)=2024 or DATE_TRUNC('month', launch_date)
            - Grouping: GROUP BY country / object_type with COUNT(*)
            - Ranges: BETWEEN or comparisons on period_min, inclination_deg, apogee_km, perigee_km
            - RCS bins: WHERE rcs_size IN ('SMALL','MEDIUM','LARGE')

            5) Examples (adjust filters to the user’s ask)
            -- Starlink payloads launched in 2024
            SELECT norad_cat_id, object_name, object_id, launch_date, launch_site
            FROM satcat
            WHERE object_type = 'PAYLOAD'
              AND object_name ILIKE '%STARLINK%'
              AND EXTRACT(YEAR FROM launch_date) = 2024
            ORDER BY launch_date, norad_cat_id
            LIMIT 200;

            -- How many objects decayed in July 2023?
            SELECT COUNT(*) AS decays_in_jul_2023
            FROM satcat
            WHERE decay_date >= DATE '2023-07-01' AND decay_date < DATE '2023-08-01';

            -- Top 10 countries by on-orbit payloads
            SELECT country, COUNT(*) AS payloads_on_orbit
            FROM satcat
            WHERE object_type = 'PAYLOAD' AND decay_date IS NULL
            GROUP BY country
            ORDER BY payloads_on_orbit DESC
            LIMIT 10;

            -- Large-RCS rocket bodies launched from Baikonur (TYMSC)
            SELECT norad_cat_id, object_name, launch_date, rcs_size
            FROM satcat
            WHERE object_type = 'ROCKET BODY'
              AND rcs_size = 'LARGE'
              AND launch_site = 'TYMSC'
            ORDER BY launch_date DESC
            LIMIT 100;

            -- Near-Earth objects (period < 225 min) with inclination > 97°
            SELECT norad_cat_id, object_name, period_min, inclination_deg, launch_date
            FROM satcat
            WHERE decay_date IS NULL
              AND period_min < 225
              AND inclination_deg > 97
            ORDER BY launch_date DESC
            LIMIT 200;
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
