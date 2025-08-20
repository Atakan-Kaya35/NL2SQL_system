package com.example.backend.dto;

import java.util.List;
import java.util.Map;

public record RunResponse(
        String sql,
        List<String> headers,
        List<List<Object>> rows,
        List<String> warnings,
        String answer) {
}
