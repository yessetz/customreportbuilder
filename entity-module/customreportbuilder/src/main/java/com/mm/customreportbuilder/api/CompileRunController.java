package com.mm.customreportbuilder.api;

import com.mm.customreportbuilder.config.FactsProperties;
import com.mm.customreportbuilder.dims.DimsRegistry;
import com.mm.customreportbuilder.facts.FactsRegistry;
import com.mm.customreportbuilder.reports.*;
import com.mm.customreportbuilder.security.SafeSqlPolicy; // if you added it earlier
import com.mm.customreportbuilder.service.ReportService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("api/reports")
public class CompileRunController {

  private final FactsRegistry facts;
  private final DimsRegistry dims;
  private final ReportService reportService;
  private final SafeSqlPolicy safeSql; // remove if you didn't add the policy; or inject nullable

  public CompileRunController(FactsRegistry facts, DimsRegistry dims, ReportService reportService, SafeSqlPolicy safeSql) {
    this.facts = facts;
    this.dims = dims;
    this.reportService = reportService;
    this.safeSql = safeSql;
  }

  @PostMapping("/statementByTemplate")
  public StatementResponse statementByTemplate(@RequestBody CompileRunRequest req) {
    if (req == null || req.kind == null || req.name == null) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing kind or name");
    }
    String baseSql = switch (req.kind.toLowerCase()) {
      case "fact" -> facts.readSql(req.name);
      case "dim"  -> dims.readSql(req.name);
      default     -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "kind must be 'fact' or 'dim'");
    };

    String compiled = QueryCompiler.compileTemplate(baseSql, req.parts != null ? req.parts : new QueryParts());

    // Optional safety: enforce read-only, limit, allowed schemas
    String sanitized = (safeSql != null) ? safeSql.sanitizeSelect(compiled) : compiled;

    var submit = reportService.submitStatement(sanitized);
    return StatementResponse.fromMap(submit);
  }
}
