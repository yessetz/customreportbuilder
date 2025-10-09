package com.mm.customreportbuilder.service.impl;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import com.mm.customreportbuilder.service.ReportService;
import com.mm.customreportbuilder.databricks.DatabricksSqlClient;
import com.mm.customreportbuilder.databricks.DatabricksSqlClient.SchemaInfo;
import com.mm.customreportbuilder.cache.ChunkCacheService;
import com.mm.customreportbuilder.cache.ViewCacheService;
import com.mm.customreportbuilder.model.aggrid.AgGridParsedModels;
import com.mm.customreportbuilder.model.aggrid.FilterDescriptor;
import com.mm.customreportbuilder.model.aggrid.SortModelEntry;
import com.mm.customreportbuilder.util.AgGridModelParser;

@Service
public class ReportServiceImpl implements ReportService {

    private static final Logger log = LoggerFactory.getLogger(ReportServiceImpl.class);

    private final DatabricksSqlClient client;
    private final ChunkCacheService cache;
    private final ViewCacheService viewCache;

    @Value("${CACHE_PAGE_SIZE:500}")
    private int PAGE_SIZE;

    @Value("${CACHE_FIRST_CHUNK_MAX_WAIT_MS:8000}")
    private long FIRST_CHUNK_MAX_WAIT_MS;

    @Value("${CACHE_FIRST_CHUNK_POLL_MS:150}")
    private long FIRST_CHUNK_POLL_MS;

    // Guardrails for view building (to avoid huge in-memory sorts if rowCount unknown)
    @Value("${VIEW_MAX_SCAN_PAGES:2000}")           // 2000 * 500 = ~1,000,000 rows cap when rowCount unknown
    private int VIEW_MAX_SCAN_PAGES;

    @Value("${VIEW_BUILD_LOG_EVERY:25}")            // log progress every N pages
    private int VIEW_BUILD_LOG_EVERY;

    public ReportServiceImpl(DatabricksSqlClient client, ChunkCacheService cache, ViewCacheService viewCache) {
        this.client = client;
        this.cache = cache;
        this.viewCache = viewCache;
    }

    // ======================== Submit & Meta (unchanged) ========================

    @Override
    public Map<String, Object> submitStatement(String sql) {
        String userId = "local";

        String statementId = client.submitStatement(sql);
        SchemaInfo schemaInfo = client.getSchema(statementId);

        // Initialize meta
        cache.putMeta(userId, statementId, PAGE_SIZE, null, schemaInfo.columnNames(), schemaInfo.columnMeta(), "PENDING");

        final AtomicInteger nextPageIndex = new AtomicInteger(0);

        client.streamChunks(statementId, PAGE_SIZE, (chunkIndex, rows, totalRows, state) -> {
            // Update meta with latest totals and state whenever available
            if (totalRows != null || state != null) {
                try {
                    cache.putMeta(userId, statementId, PAGE_SIZE, totalRows, null, null, state);
                } catch (Exception e) {
                    log.warn("Failed to update meta for statementId={} during stream: {}", statementId, e.toString());
                }
            }

            // chunkIndex == -1 is used for meta/state notifications — skip data handling
            if (chunkIndex >= 0 && rows != null && !rows.isEmpty()) {
                // Re-slice the Databricks DB chunk into PAGE_SIZE pages
                for (int offset = 0; offset < rows.size(); offset += PAGE_SIZE) {
                    int to = Math.min(offset + PAGE_SIZE, rows.size());
                    int pageIdx = nextPageIndex.getAndIncrement();

                    List<List<Object>> pageRows = rows.subList(offset, to);
                    cache.putChunk(userId, statementId, pageIdx, pageRows);

                    if (pageIdx % 20 == 0) {
                        log.debug("STORED base page chunk={} rows={} statementId={} (from dbChunkIndex={})",
                                pageIdx, pageRows.size(), statementId, chunkIndex);
                    }
                }
            }
        });

        Map<String, Object> out = new HashMap<>();
        out.put("statementId", statementId);
        out.put("pageSize", PAGE_SIZE);
        return out;
    }

    @Override
    public Map<String, Object> getStatementMeta(String statementId) {
        String userId = "local";
        Map<String, Object> meta = cache.getMeta(userId, statementId);
        if (meta == null) {
            return Map.of("statementId", statementId, "state", "PENDING");
        }
        Map<String, Object> out = new HashMap<>(meta);
        out.put("statementId", statementId);
        return out;
    }

    // ======================== Base rows (unchanged) ========================

    @Override
    public Map<String, Object> getRows(String statementId, int startRow, int endRow) {
        String userId = "local";
        Map<String, Object> meta = cache.getMeta(userId, statementId);

        if (meta == null) {
            return Map.of("rows", List.of(), "lastRow", null);
        }

        int pageSize = ((Number) meta.getOrDefault("pageSize", PAGE_SIZE)).intValue();
        Integer rowCount = safeInt(meta.get("rowCount"));

        if (endRow <= startRow) {
            return Map.of("rows", List.of(), "lastRow", rowCount);
        }

        int firstChunk = Math.max(0, startRow / pageSize);
        int lastChunk = Math.max(firstChunk, (endRow - 1) / pageSize);

        final long deadline = System.currentTimeMillis() + Math.max(0L, FIRST_CHUNK_MAX_WAIT_MS);
        List<List<Object>> buffer = new ArrayList<>();

        for (int index = firstChunk; index <= lastChunk; index++) {
            List<List<Object>> chunk = cache.getChunk(userId, statementId, index);

            if (chunk == null || chunk.isEmpty()) {
                chunk = waitForChunk(userId, statementId, index, deadline, Math.max(1L, FIRST_CHUNK_POLL_MS));
            }

            if (chunk != null && !chunk.isEmpty()) {
                buffer.addAll(chunk);
            } else {
                break;
            }
        }

        int offset = startRow - (firstChunk * pageSize);
        int toTake = Math.max(0, endRow - startRow);
        List<List<Object>> page;

        if (offset < 0 || buffer.isEmpty() || offset >= buffer.size()) {
            page = List.of();
        } else {
            int from = Math.min(offset, buffer.size());
            int to = Math.min(from + toTake, buffer.size());
            page = buffer.subList(from, to);
        }

        return Map.of("rows", page, "lastRow", rowCount);
    }

    // ======================== View rows (new behavior) ========================

    // This overload is declared in ReportService and called by the controller.
    @Override
    public Map<String, Object> getRows(String statementId, int startRow, int endRow, String sortModelJson, String filterModelJson) {
        String userId = "local";

        // Parse models; if empty => behave like base
        AgGridParsedModels parsed = AgGridModelParser.parse(sortModelJson, filterModelJson, null);
        boolean hasSort = parsed.getSortModel() != null && !parsed.getSortModel().isEmpty();
        boolean hasFilter = parsed.getFilterModel() != null && !parsed.getFilterModel().isEmpty();

        if (!hasSort && !hasFilter) {
            // No models → return base pages
            return getRows(statementId, startRow, endRow);
        }

        // Compute signature for this (stmt, sort, filter)
        String sig = viewCache.computeSignature(statementId, parsed.getCanonicalSortJson(), parsed.getCanonicalFilterJson());

        // If the view exists, slice and return
        Map<String, Object> viewMeta = viewCache.getMeta(userId, statementId, sig);
        if (viewMeta != null) {
            int ps = ((Number) viewMeta.getOrDefault("pageSize", PAGE_SIZE)).intValue();
            return sliceFromView(userId, statementId, sig, startRow, endRow, ps);
        }

        // Build the view (first time) from base pages
        Map<String, Object> baseMeta = cache.getMeta(userId, statementId);
        if (baseMeta == null) {
            // Base not ready → empty
            return Map.of("rows", List.of(), "lastRow", null);
        }

        // Columns & indexes
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) baseMeta.getOrDefault("columns", List.of());
        Map<String, Integer> colIndex = indexColumns(columns);
        Integer rowCount = safeInt(baseMeta.get("rowCount"));
        int pageSize = ((Number) baseMeta.getOrDefault("pageSize", PAGE_SIZE)).intValue();

        // Build guardrail: if rowCount unknown, cap scan by VIEW_MAX_SCAN_PAGES
        int maxPagesToScan = (rowCount != null) ? Math.max(0, (rowCount + pageSize - 1) / pageSize)
                                                : VIEW_MAX_SCAN_PAGES;

        // Coerce allowed columns for parsing (now we know columns)
        parsed = AgGridModelParser.parse(sortModelJson, filterModelJson, colIndex.keySet());
        hasSort = parsed.getSortModel() != null && !parsed.getSortModel().isEmpty();
        hasFilter = parsed.getFilterModel() != null && !parsed.getFilterModel().isEmpty();
        if (!hasSort && !hasFilter) {
            // After column validation, nothing left → base
            return getRows(statementId, startRow, endRow);
        }

        // 1) Read base pages, apply filters on the fly, collect into memory
        List<List<Object>> filtered = new ArrayList<>(Math.min(rowCount != null ? rowCount : 10000, 200000));
        int scanned = 0;
        for (int pageIdx = 0; pageIdx < maxPagesToScan; pageIdx++) {
            List<List<Object>> chunk = cache.getChunk(userId, statementId, pageIdx);
            if (chunk == null || chunk.isEmpty()) {
                // If rowCount is known we can break early once we covered all pages
                if (rowCount != null && pageIdx >= maxPagesToScan - 1) break;
                // else: keep scanning until cap
            } else {
                // Filter rows
                for (List<Object> row : chunk) {
                    if (rowMatchesFilters(row, parsed.getFilterModel(), colIndex)) {
                        filtered.add(row);
                    }
                }
            }

            scanned++;
            if (VIEW_BUILD_LOG_EVERY > 0 && pageIdx % VIEW_BUILD_LOG_EVERY == 0) {
                log.debug("View build scanning base page {} (sig={}, stmt={})", pageIdx, sig, statementId);
            }

            // Early exit if rowCount known and we’ve read all pages
            if (rowCount != null && pageIdx >= maxPagesToScan - 1) break;
        }

        // If we hit scan cap with unknown rowCount, we return base (fallback).
        if (rowCount == null && scanned >= VIEW_MAX_SCAN_PAGES) {
            log.warn("View build hit scan cap ({} pages) for stmt={}, sig={}. Falling back to base.", VIEW_MAX_SCAN_PAGES, statementId, sig);
            return getRows(statementId, startRow, endRow);
        }

        // 2) Sort if needed
        if (hasSort) {
            Comparator<List<Object>> cmp = buildComparator(parsed.getSortModel(), colIndex);
            filtered.sort(cmp);
        }

        // 3) Store as view pages in Redis
        int total = filtered.size();
        int totalChunks = (total + pageSize - 1) / pageSize;
        viewCache.putMeta(userId, statementId, sig, pageSize, total, Map.of("chunkCount", totalChunks));

        for (int i = 0; i < totalChunks; i++) {
            int from = i * pageSize;
            int to = Math.min(from + pageSize, total);
            List<List<Object>> page = filtered.subList(from, to);
            viewCache.putChunk(userId, statementId, sig, i, page);
        }

        log.debug("View built for stmt={} sig={} rows={} chunks={}", statementId, sig, total, totalChunks);

        // 4) Serve the requested slice from the freshly built view
        return sliceFromView(userId, statementId, sig, startRow, endRow, pageSize);
    }

    // ======================== Eviction (unchanged) ========================

    @Override
    public void evict(String statementId) {
        cache.invalidateStatement("local", statementId);
        // Views for the statement will be lazily overwritten on next build; you can add a bulk
        // SCAN-based eviction here if you want hard cleanup of all views for this statement.
    }

    // ======================== Helpers ========================

    private Map<String, Object> sliceFromView(String userId, String statementId, String sig, int startRow, int endRow, int pageSize) {
        Map<String, Object> meta = viewCache.getMeta(userId, statementId, sig);
        if (meta == null) return Map.of("rows", List.of(), "lastRow", null);

        Integer rowCount = safeInt(meta.get("rowCount"));
        if (endRow <= startRow) {
            return Map.of("rows", List.of(), "lastRow", rowCount);
        }

        int firstChunk = Math.max(0, startRow / pageSize);
        int lastChunk = Math.max(firstChunk, (endRow - 1) / pageSize);

        List<List<Object>> buffer = new ArrayList<>();
        for (int index = firstChunk; index <= lastChunk; index++) {
            List<List<Object>> chunk = viewCache.getChunk(userId, statementId, sig, index);
            if (chunk != null && !chunk.isEmpty()) {
                buffer.addAll(chunk);
            } else {
                break; // view should be contiguous; missing means end
            }
        }

        int offset = startRow - (firstChunk * pageSize);
        int toTake = Math.max(0, endRow - startRow);
        List<List<Object>> page;

        if (offset < 0 || buffer.isEmpty() || offset >= buffer.size()) {
            page = List.of();
        } else {
            int from = Math.min(offset, buffer.size());
            int to = Math.min(from + toTake, buffer.size());
            page = buffer.subList(from, to);
        }

        return Map.of("rows", page, "lastRow", rowCount);
    }

    private Map<String, Integer> indexColumns(List<String> columns) {
        Map<String, Integer> m = new HashMap<>();
        if (columns == null) return m;
        for (int i = 0; i < columns.size(); i++) {
            String c = columns.get(i);
            if (c == null) continue;
            m.put(c, i);                               // original case
            m.put(c.toLowerCase(Locale.ROOT), i);      // lowercase alias
        }
        return m;
    }

    private Integer safeInt(Object o) {
        if (o == null) return null;
        if (o instanceof Integer) return (Integer) o;
        if (o instanceof Long) return (int) ((Long) o).longValue();
        if (o instanceof Number) return ((Number) o).intValue();
        try { return Integer.parseInt(String.valueOf(o)); } catch (Exception e) { return null; }
    }

    private List<List<Object>> waitForChunk(String userId, String statementId, int index, long deadlineMs, long pollMs) {
        while (System.currentTimeMillis() < deadlineMs) {
            List<List<Object>> chunk = cache.getChunk(userId, statementId, index);
            if (chunk != null && !chunk.isEmpty()) {
                return chunk;
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return null;
    }

    // ---------------- Filtering ----------------

    private boolean rowMatchesFilters(List<Object> row, Map<String, FilterDescriptor> filterModel, Map<String, Integer> colIndex) {
        if (filterModel == null || filterModel.isEmpty()) return true;
        for (Map.Entry<String, FilterDescriptor> e : filterModel.entrySet()) {
            String colId = e.getKey();
            Integer idx = colIndex.get(colId);
            if (idx == null && colId != null) idx = colIndex.get(colId.toLowerCase(Locale.ROOT));
            if (idx == null || idx < 0 || idx >= row.size()) continue; // unknown column => ignore
            Object val = row.get(idx);
            if (!evalFilter(val, e.getValue())) return false;
        }
        return true;
    }

    private boolean evalFilter(Object cell, FilterDescriptor fd) {
        if (fd == null) return true;

        // Compound
        if (fd.isCompound()) {
            boolean and = "AND".equalsIgnoreCase(fd.getOperator());
            if (fd.getConditions() == null || fd.getConditions().isEmpty()) return true;
            if (and) {
                for (FilterDescriptor c : fd.getConditions()) {
                    if (!evalFilter(cell, c)) return false;
                }
                return true;
            } else {
                for (FilterDescriptor c : fd.getConditions()) {
                    if (evalFilter(cell, c)) return true;
                }
                return false;
            }
        }

        String type = safeLower(fd.getType());
        String filterType = safeLower(fd.getFilterType());

        // Text filters
        if ("text".equals(filterType)) {
            String s = toStringOrNull(cell);
            String q = fd.getFilter();
            if (q == null) q = "";
            String sL = s == null ? "" : s.toLowerCase(Locale.ROOT);
            String qL = q.toLowerCase(Locale.ROOT);
            return switch (type) {
                case "contains" -> sL.contains(qL);
                case "notcontains" -> !sL.contains(qL);
                case "equals" -> sL.equals(qL);
                case "notequals" -> !sL.equals(qL);
                case "startswith" -> sL.startsWith(qL);
                case "endswith" -> sL.endsWith(qL);
                default -> true; // unknown -> pass
            };
        }

        // Number filters
        if ("number".equals(filterType)) {
            BigDecimal n = toNumber(cell);
            BigDecimal a = toNumber(fd.getFilter());
            BigDecimal b = toNumber(fd.getFilterTo());
            int cmp = (n == null || a == null) ? Integer.MIN_VALUE : n.compareTo(a);
            return switch (type) {
                case "equals" -> n != null && a != null && cmp == 0;
                case "notequals" -> n != null && a != null && cmp != 0;
                case "greaterthan" -> n != null && a != null && cmp > 0;
                case "greaterthanequal" -> n != null && a != null && cmp >= 0;
                case "lessthan" -> n != null && a != null && cmp < 0;
                case "lessthanequal" -> n != null && a != null && cmp <= 0;
                case "inrange" -> {
                    if (n == null || a == null || b == null) yield true;
                    int c1 = n.compareTo(a);
                    int c2 = n.compareTo(b);
                    yield (c1 >= 0 && c2 <= 0);
                }
                default -> true;
            };
        }

        // Date filters (expects "yyyy-MM-dd" or ISO-like strings)
        if ("date".equals(filterType)) {
            LocalDate d = toDate(cell);
            LocalDate a = toDate(fd.getDateFrom() != null ? fd.getDateFrom() : fd.getFilter());
            LocalDate b = toDate(fd.getDateTo());
            if (d == null || a == null) return true;
            return switch (type) {
                case "equals" -> d.isEqual(a);
                case "notequals" -> !d.isEqual(a);
                case "greaterthan" -> d.isAfter(a);
                case "greaterthanequal" -> !d.isBefore(a);
                case "lessthan" -> d.isBefore(a);
                case "lessthanequal" -> !d.isAfter(a);
                case "inrange" -> (b != null) ? (!d.isBefore(a) && !d.isAfter(b)) : true;
                default -> true;
            };
        }

        // Unknown filter types pass
        return true;
    }

    private String safeLower(String s) {
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }

    private String toStringOrNull(Object o) {
        if (o == null) return null;
        return String.valueOf(o);
    }

    private BigDecimal toNumber(Object o) {
        if (o == null) return null;
        try {
            if (o instanceof BigDecimal bd) return bd;
            if (o instanceof Integer i) return new BigDecimal(i);
            if (o instanceof Long l) return new BigDecimal(l);
            if (o instanceof Double d) return BigDecimal.valueOf(d);
            if (o instanceof Float f) return BigDecimal.valueOf(f.doubleValue());
            String s = String.valueOf(o).replaceAll(",", "").trim();
            if (s.isEmpty() || "null".equalsIgnoreCase(s)) return null;
            return new BigDecimal(s);
        } catch (Exception e) {
            return null;
        }
    }

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private LocalDate toDate(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o).trim();
        if (s.isEmpty() || "null".equalsIgnoreCase(s)) return null;
        try {
            if (s.length() >= 10) {
                return LocalDate.parse(s.substring(0, 10), DATE_FMT);
            }
        } catch (Exception ignore) {}
        return null;
    }

    // ---------------- Sorting ----------------

    private Comparator<List<Object>> buildComparator(List<SortModelEntry> sorts, Map<String, Integer> colIndex) {
        if (sorts == null || sorts.isEmpty()) {
            return (a, b) -> 0;
        }
        List<Comparator<List<Object>>> comparators = new ArrayList<>();
        for (SortModelEntry s : sorts) {
            String key = s.getColId();
            Integer idx = key == null ? null : colIndex.getOrDefault(key, colIndex.get(key.toLowerCase(Locale.ROOT)));
            if (idx == null) continue;
            boolean asc = s.isAsc();
            comparators.add((r1, r2) -> {
                Object o1 = idx < r1.size() ? r1.get(idx) : null;
                Object o2 = idx < r2.size() ? r2.get(idx) : null;
                int c = compareCells(o1, o2);
                return asc ? c : -c;
            });
        }
        return (r1, r2) -> {
            for (Comparator<List<Object>> c : comparators) {
                int x = c.compare(r1, r2);
                if (x != 0) return x;
            }
            return 0;
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int compareCells(Object a, Object b) {
        if (a == b) return 0;
        if (a == null) return 1; // nulls last
        if (b == null) return -1;

        // Try numeric
        BigDecimal na = toNumber(a);
        BigDecimal nb = toNumber(b);
        if (na != null && nb != null) {
            return na.compareTo(nb);
        }

        // Try date
        LocalDate da = toDate(a);
        LocalDate db = toDate(b);
        if (da != null && db != null) {
            return da.compareTo(db);
        }

        // Fall back to string
        String sa = String.valueOf(a);
        String sb = String.valueOf(b);
        return sa.compareToIgnoreCase(sb);
    }
}
