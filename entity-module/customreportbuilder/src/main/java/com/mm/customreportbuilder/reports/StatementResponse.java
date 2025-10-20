package com.mm.customreportbuilder.reports;

import java.util.Map;

public class StatementResponse {
  public String statementId;
  public StatementResponse() {}
  public StatementResponse(String id) { this.statementId = id; }

  public static StatementResponse fromMap(Map<String, Object> m) {
    return new StatementResponse(String.valueOf(m.get("statementId")));
  }
}
