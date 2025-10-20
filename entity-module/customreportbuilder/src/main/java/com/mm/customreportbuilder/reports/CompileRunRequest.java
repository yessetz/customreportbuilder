package com.mm.customreportbuilder.reports;

public class CompileRunRequest {
  public String kind;   // "fact" or "dim"
  public String name;   // e.g., "fact_product" or "categories"
  public QueryParts parts;
}
