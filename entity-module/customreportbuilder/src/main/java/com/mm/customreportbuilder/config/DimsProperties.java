package com.mm.customreportbuilder.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "app")
public class DimsProperties {

    private Map<String, DimConfig> dims = new HashMap<>();

    public Map<String, DimConfig> getDims() { return dims; }
    public void setDims(Map<String, DimConfig> dims) { this.dims = dims; }

    public static class DimConfig {
        private String path;       // e.g., classpath:sql/dims/categories.sql
        private Integer ttlSeconds; // optional for caching

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
        public Integer getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(Integer ttlSeconds) { this.ttlSeconds = ttlSeconds; }
    }
}
