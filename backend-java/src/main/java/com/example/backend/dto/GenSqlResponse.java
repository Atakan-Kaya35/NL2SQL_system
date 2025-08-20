package com.example.backend.dto;

import java.util.List;

public record GenSqlResponse(String sql, String dialect, List<String> warnings) {
}
