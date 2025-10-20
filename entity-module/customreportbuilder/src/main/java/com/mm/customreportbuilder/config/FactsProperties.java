package com.mm.customreportbuilder.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "app")
public class FactsProperties {
  private Map<String, FactConfig> facts = new HashMap<>();
  public Map<String, FactConfig> getFacts() { return facts; }
  public void setFacts(Map<String, FactConfig> facts) { this.facts = facts; }

  public static class FactConfig {
    private String path; // classpath:sql/facts/...
    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
  }
}
