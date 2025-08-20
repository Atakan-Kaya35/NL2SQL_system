package com.example.backend.dto;

import java.util.List;
import java.util.Map;

public record QueryResponse(String sql, List<Map<String, Object>> rows, List<String> warnings) {}
