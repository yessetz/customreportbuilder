package com.mm.customreportbuilder.facts;

import com.mm.customreportbuilder.config.FactsProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

@Component
public class FactsRegistry {
  private final Map<String, FactsProperties.FactConfig> defs;
  public FactsRegistry(FactsProperties props) { this.defs = props.getFacts(); }

  public Set<String> names() { return defs.keySet(); }

  public String readSql(String name) {
    var cfg = defs.get(name);
    if (cfg == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown fact: " + name);
    String cp = cfg.getPath().startsWith("classpath:")
        ? cfg.getPath().substring("classpath:".length())
        : cfg.getPath();
    try (InputStream in = new ClassPathResource(cp).getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Cannot read SQL for fact: " + name, e);
    }
  }
}
