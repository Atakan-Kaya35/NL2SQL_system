package com.example.backend.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SchemaService {
    private final JdbcTemplate jdbc;

    public SchemaService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public String ddlSnapshot() {
        // Minimal schema grounding: list tables and columns
        String ddl = """
        -- Tables and columns (sample snapshot)
        """;
        List<String> rows = jdbc.query(
            "redacted",
            (rs, i) -> rs.getString("table_name") + "(" + rs.getString("column_name") + " " + rs.getString("data_type") + ")"
        );
        return ddl + String.join("\n", rows);
    }
}
