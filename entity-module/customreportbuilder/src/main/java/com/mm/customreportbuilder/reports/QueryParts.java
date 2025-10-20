package com.mm.customreportbuilder.reports;

import java.util.List;

public class QueryParts {
  public List<JoinClause> joins;
  public List<WhereClause> wheres;
  public List<String> groupBy;
  public List<String> having;
  public List<String> orderBy;
  public Integer limit;

  public static class JoinClause { public String key; public String sql; }
  public static class WhereClause { public String key; public String sql; }
}
