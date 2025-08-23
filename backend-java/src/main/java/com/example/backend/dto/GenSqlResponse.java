package com.example.backend.dto;

import java.util.List;

public record GenSqlResponse(String response, String dialect, List<String> warnings) {
}
