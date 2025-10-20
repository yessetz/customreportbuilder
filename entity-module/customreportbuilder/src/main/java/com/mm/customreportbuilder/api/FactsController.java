package com.mm.customreportbuilder.api;

import com.mm.customreportbuilder.facts.FactsRegistry;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
@RequestMapping("api/facts")
public class FactsController {
  private final FactsRegistry registry;
  public FactsController(FactsRegistry registry) { this.registry = registry; }

  // List available fact names (handy for QA)
  @GetMapping
  public Set<String> list() { return registry.names(); }

  // Return the raw SQL template for a fact (text/plain)
  @GetMapping(value = "{name}.sql", produces = MediaType.TEXT_PLAIN_VALUE)
  public String sql(@PathVariable String name) {
    return registry.readSql(name);
  }
}
