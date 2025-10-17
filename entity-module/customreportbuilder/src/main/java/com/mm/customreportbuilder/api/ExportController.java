package com.mm.customreportbuilder.api;

import com.mm.customreportbuilder.service.ReportService;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("api/reports/export")
public class ExportController {

    private final ReportService reportService;

    public ExportController(ReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping(value = "/csv", produces = "text/csv")
    public ResponseEntity<StreamingResponseBody> exportCsv(
        @RequestParam String statementId,
        @RequestParam(required = false, defaultValue = "true") boolean header,
        @RequestParam(required = false, defaultValue = "false") boolean bom, // Excel-friendly
        @RequestParam(required = false) String sortModel,
        @RequestParam(required = false) String filterModel  
    ) {
        // 1) Get meta (columns, rowCount, pageSize)
        Map<String, Object> meta = reportService.getStatementMeta(statementId);
        List<String> columns = extractColumns(meta);
        int pageSize = (meta.get("pageSize") instanceof Number)
            ? ((Number) meta.get("pageSize")).intValue() : 500;
        Integer rowCount = (meta.get("rowCount") instanceof Number)
            ? ((Number) meta.get("rowCount")).intValue() : null; // may be null

        StreamingResponseBody body = out -> {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 64 * 1024)) {
            // Optional BOM for Excel
            if (bom) {
            bw.write('\uFEFF');
            }
            // 2) Header
            if (header && !columns.isEmpty()) {
            bw.write(csvLine(columns));
            bw.write("\r\n");
            }
            // 3) Stream rows in blocks
            int start = 0;
            int block = (pageSize > 0 ? pageSize : 500);
            while (true) {
            int end = start + block;
            //Map<String, Object> res = reportService.getRows(statementId, start, end);
            Map<String, Object> res = getRowsWithModels(statementId, start, end, sortModel, filterModel);
            List<?> rows = extractRows(res);
            if (rows == null || rows.isEmpty()) break;

            for (Object r : rows) {
                List<String> values = rowToValues(r, columns);
                bw.write(csvLine(values));
                bw.write("\r\n");
            }
            bw.flush();
            start = end;
            if (rowCount != null && start >= rowCount) break;
            }
        }
        };

        String filename = statementId + ".csv";
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .contentType(MediaType.parseMediaType("text/csv"))
            .body(body);
    }

    // ----- helpers -----

    @SuppressWarnings("unchecked")
    private static List<String> extractColumns(Map<String, Object> meta) {
        Object cols = meta.get("columns");
        if (cols instanceof List) {
        List<?> list = (List<?>) cols;
        return list.stream().map(String::valueOf).collect(Collectors.toList());
        }
        Object schema = meta.get("schema");
        if (schema instanceof List) {
        List<Map<String, Object>> s = (List<Map<String, Object>>) schema;
        s.sort(Comparator.comparingInt(m -> ((Number) (m.getOrDefault("position", 0))).intValue()));
        List<String> out = new ArrayList<>(s.size());
        for (Map<String, Object> c : s) out.add(String.valueOf(c.get("name")));
        return out;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private static List<?> extractRows(Map<String, Object> res) {
        if (res == null) return List.of();
        Object rows = (res.get("rows") != null) ? res.get("rows")
                : (res.get("data") != null) ? res.get("data")
                : (res.get("result") != null) ? res.get("result") : null;
        if (rows instanceof List) return (List<?>) rows;

        Object chunks = res.get("chunks");
        if (chunks instanceof List) {
        for (Object ch : (List<?>) chunks) {
            if (ch instanceof Map) {
            Object rr = ((Map<?, ?>) ch).get("rows");
            if (rr instanceof List && !((List<?>) rr).isEmpty()) return (List<?>) rr;
            }
        }
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private static List<String> rowToValues(Object row, List<String> columns) {
        if (row instanceof List) {
        List<?> arr = (List<?>) row;
        List<String> vals = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Object v = (i < arr.size()) ? arr.get(i) : null;
            vals.add(stringify(v));
        }
        return vals;
        }
        if (row instanceof Map) {
        Map<String, Object> m = (Map<String, Object>) row;
        List<String> vals = new ArrayList<>(columns.size());
        for (String c : columns) vals.add(stringify(m.get(c)));
        return vals;
        }
        return List.of();
    }

    private static String stringify(Object v) {
        if (v == null) return "";
        // If you prefer JSON for nested structs/arrays, swap to Jackson here
        return String.valueOf(v);
    }

    private static String csvLine(List<String> values) {
        return values.stream().map(ExportController::quoteCsv).collect(Collectors.joining(","));
    }

    private static String quoteCsv(String s) {
        if (s == null) s = "";
        boolean needs = s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r");
        if (needs) s = "\"" + s.replace("\"", "\"\"") + "\"";
        return s;
    }
    
    // Try the 5-arg overload; fall back to the 3-arg one if your ReportService doesn’t have it yet.
    private Map<String, Object> getRowsWithModels(String statementId, int start, int end, String sortModel, String filterModel) {
        try {
            // If you already have this overload, this will work as-is:
            return reportService.getRows(statementId, start, end, sortModel, filterModel);
        } catch (NoSuchMethodError | NoSuchMethodException | RuntimeException e) {
            // Fallback: original signature without models
            return reportService.getRows(statementId, start, end);
        }
    }
}
