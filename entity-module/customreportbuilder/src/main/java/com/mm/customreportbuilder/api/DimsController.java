package com.mm.customreportbuilder.api;

import com.mm.customreportbuilder.dims.DimsRegistry;
import com.mm.customreportbuilder.service.ReportService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@RestController
@RequestMapping("api/dims")
public class DimsController {

    private final ReportService reportService;
    private final DimsRegistry registry;

    public DimsController(ReportService reportService, DimsRegistry registry) {
        this.reportService = reportService;
        this.registry = registry;
    }

    // List available dims (handy for debugging/UX)
    @GetMapping
    public Set<String> list() {
        return registry.names();
    }

    // Generic endpoint: /api/dims/{name}
    @GetMapping("{name}")
    public List<Map<String, String>> dim(@PathVariable String name) {
        String sql = registry.readSql(name);
        return runSmall(sql);
    }

    // ---- shared helpers ----

    private List<Map<String, String>> runSmall(String sql) {
        Map<String, Object> submit = reportService.submitStatement(sql);
        String statementId = String.valueOf(submit.get("statementId"));

        // Poll meta (short)
        long deadline = System.currentTimeMillis() + 15_000;
        Map<String, Object> meta;
        while (true) {
            meta = reportService.getStatementMeta(statementId);
            Object rowCountObj = meta.get("rowCount");
            String state = String.valueOf(meta.getOrDefault("state", "")).toUpperCase(Locale.ROOT);
            if (rowCountObj instanceof Number) break;
            if (state.equals("SUCCEEDED") || state.equals("DONE") || state.equals("COMPLETED")) break;
            if (System.currentTimeMillis() > deadline) {
                throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "Timed out loading dim data");
            }
            try { Thread.sleep(200); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        int rowCount = (meta.get("rowCount") instanceof Number)
                ? ((Number) meta.get("rowCount")).intValue() : 1000;

        Map<String, Object> rowsResp = reportService.getRows(statementId, 0, Math.max(1000, rowCount));
        List<?> rows = extractRows(rowsResp);

        List<Map<String, String>> out = new ArrayList<>();
        for (Object r : rows) {
            if (r instanceof List) {
                List<?> arr = (List<?>) r;
                String id = arr.size() > 0 ? String.valueOf(arr.get(0)) : null;
                String name = arr.size() > 1 ? String.valueOf(arr.get(1)) : null;
                if (id != null && !id.isBlank() && name != null && !name.isBlank()) {
                    out.add(Map.of("id", id, "name", name));
                }
            } else if (r instanceof Map) {
                Map<?, ?> m = (Map<?, ?>) r;
                Object id = m.get("id");
                Object name = m.get("name");
                if (id != null && name != null) {
                    out.add(Map.of("id", String.valueOf(id), "name", String.valueOf(name)));
                }
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private List<?> extractRows(Map<String, Object> res) {
        Object rows = (res != null) ? (
                res.get("rows") != null ? res.get("rows") :
                res.get("data") != null ? res.get("data") :
                res.get("result") != null ? res.get("result") : null
        ) : null;
        return (rows instanceof List) ? (List<?>) rows : List.of();
    }
}
