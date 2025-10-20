package com.mm.customreportbuilder.reports;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class QueryCompiler {
  private QueryCompiler() {}

  private static String replaceMarker(String sql, String markerPrefix, String key, String text) {
    String marker = "/*" + markerPrefix + ":" + key + "*/";
    if (sql.contains(marker)) {
      return sql.replace(marker, (text != null && !text.isBlank()) ? (text + "\n") : "");
    }
    return null; // no-op
  }

  public static String compileTemplate(String baseSql, QueryParts parts) {
    String sql = baseSql;

    // ---------- JOINS ----------
    if (parts.joins != null) {
      for (QueryParts.JoinClause j : parts.joins) {
        if (j == null || j.sql == null || j.sql.isBlank()) continue;
        String repl = replaceMarker(sql, "JOIN", j.key, j.sql);
        if (repl != null) { sql = repl; continue; }
        // Fallback: insert before the first WHERE (any casing)
        Matcher m = Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (m.find()) {
          sql = sql.substring(0, m.start()) + "\n" + j.sql + "\n" + sql.substring(m.start());
        } else {
          sql = sql.replaceFirst("\\s*$", "") + "\n" + j.sql + "\n";
        }
      }
    }

    // ---------- WHEREs ----------
    Map<String, List<String>> grouped = new LinkedHashMap<>();
    if (parts.wheres != null) {
      for (QueryParts.WhereClause w : parts.wheres) {
        if (w == null || w.sql == null || w.sql.isBlank()) continue;
        grouped.computeIfAbsent(w.key, k -> new ArrayList<>()).add(w.sql);
      }
    }
    for (Map.Entry<String, List<String>> e : grouped.entrySet()) {
      String key = e.getKey();
      String combined = "  AND " + String.join("\n  AND ", e.getValue());

      String repl = replaceMarker(sql, "WHERE", key, combined);
      if (repl != null) { sql = repl; continue; }

      // Prefer inserting AFTER "WHERE 1=1"
      Matcher m11 = Pattern.compile("\\bWHERE\\b\\s*1\\s*=\\s*1", Pattern.CASE_INSENSITIVE).matcher(sql);
      if (m11.find()) {
        sql = new StringBuilder(sql).replace(m11.start(), m11.end(),
                sql.substring(m11.start(), m11.end()) + "\n" + combined).toString();
        continue;
      }

      // Otherwise insert right after WHERE token
      Matcher mW = Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE).matcher(sql);
      if (mW.find()) {
        int idx = mW.end();
        sql = sql.substring(0, idx) + "\n" + combined + "\n" + sql.substring(idx);
      } else {
        // No WHERE at all → create one
        sql = sql.replaceFirst("\\s*$", "") + "\nWHERE 1=1\n" + combined + "\n";
      }
    }

    // Normalize "WHERE\nAND ..." → "WHERE 1=1\n  AND ..."
    sql = sql.replaceAll("(?i)\\bWHERE\\s*\\n\\s*AND\\b", "WHERE 1=1\n  AND");

    // ---------- GROUP BY / HAVING / ORDER BY / LIMIT ----------
    sql = injectBlock(sql, "GROUP_BY", parts.groupBy == null || parts.groupBy.isEmpty()
      ? null : "GROUP BY\n  " + String.join(", ", parts.groupBy));

    sql = injectBlock(sql, "HAVING", parts.having == null || parts.having.isEmpty()
      ? null : "HAVING\n  " + String.join("\n  AND ", parts.having));

    sql = injectBlock(sql, "ORDER_BY", parts.orderBy == null || parts.orderBy.isEmpty()
      ? null : "ORDER BY\n  " + String.join(", ", parts.orderBy));

    if (parts.limit != null && parts.limit >= 0) {
      sql = injectBlock(sql, "LIMIT", "LIMIT " + parts.limit);
    } else {
      sql = injectBlock(sql, "LIMIT", null);
    }

    // Light whitespace tidy
    sql = sql.replaceAll("[ \\t]+\\n", "\n").replaceAll("\\n{3,}", "\n\n");
    return sql.trim() + "\n";
  }

  private static String injectBlock(String sql, String key, String block) {
    String marker = "/*" + key + "*/";
    if (sql.contains(marker)) {
      return sql.replace(marker, (block != null && !block.isBlank()) ? (block + "\n") : "");
    }
    return sql; // marker absent → leave as-is
  }
}
