package com.mm.customreportbuilder.security;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

@Component
public class SafeSqlPolicy {
  private static final Pattern FORBIDDEN = Pattern.compile(
      "(?i)\\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|REPLACE|GRANT|REVOKE|COPY\\s+INTO|OPTIMIZE|VACUUM|MSCK|RESTORE|COMMENT\\s+ON|SET\\s+|UNSET\\s+|USE\\s+|REFRESH\\s+TABLE|CACHE\\s+TABLE|UNCACHE\\s+TABLE|ANALYZE\\s+TABLE|STREAMING|RUN\\s+)\\b"
  );
  private static final Pattern SELECT_OR_WITH_START = Pattern.compile("^(?s)\\s*(SELECT|WITH)\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern LIMIT_PATTERN = Pattern.compile("(?i)\\bLIMIT\\b");

  private static final int MAX_SQL_LENGTH = 20000;
  private final List<String> allowedSchemas = List.of("analytics.mm."); // tune to your env

  public String sanitizeSelect(String sql) {
    if (sql == null || sql.isBlank()) bad("Empty SQL");
    if (sql.length() > MAX_SQL_LENGTH) bad("SQL too long");
    if (sql.indexOf(';') >= 0) bad("Multiple statements are not allowed");
    if (!SELECT_OR_WITH_START.matcher(sql).find()) bad("Only SELECT/CTE statements are allowed");
    if (FORBIDDEN.matcher(sql).find()) bad("Statement contains forbidden keywords");

    String lower = sql.toLowerCase(Locale.ROOT);
    boolean touchesAllowed = allowedSchemas.stream().anyMatch(lower::contains);
    if (!touchesAllowed) bad("Statement must reference allowed schemas");

    if (!LIMIT_PATTERN.matcher(sql).find()) {
      sql = sql.trim() + "\nLIMIT 5000";
    }
    return sql;
  }

  private static String bad(String m) {
    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsafe SQL: " + m);
  }
}
