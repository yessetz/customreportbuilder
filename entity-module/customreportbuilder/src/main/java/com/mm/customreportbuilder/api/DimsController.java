package com.mm.customreportbuilder.api;

import com.mm.customreportbuilder.dims.DimsRegistry;
import com.mm.customreportbuilder.dims.DimCache;
import com.mm.customreportbuilder.service.ReportService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

@RestController
@RequestMapping("api/dims")
public class DimsController {

    private final ReportService reportService;
    private final DimsRegistry registry;
    private final DimCache dimCache;

    public DimsController(ReportService reportService, DimsRegistry registry, DimCache dimCache) {
        this.reportService = reportService;
        this.registry = registry;
        this.dimCache = dimCache;
    }

    // List available dims (handy for debugging/UX)
    @GetMapping
    public Set<String> list() {
        return registry.names();
    }

    // Generic endpoint: /api/dims/{name}
    @GetMapping("{name}")
    public List<Map<String, String>> dim(
        @PathVariable String name,
        @RequestParam(required = false) String companyId,
        @RequestParam(required = false) String cluster,
        @RequestParam(required = false) String userId,
        HttpServletRequest req
    ) {
        // Resolve scope with fallbacks and optional headers
        String u  = firstNonBlank(userId, header(req, "X-User-Id"), "local");
        String co = firstNonBlank(companyId, header(req, "X-Company-Id"), "default_company");
        String cl = firstNonBlank(cluster, header(req, "X-Cluster-Id"), "1");

        final int ttl = Optional.ofNullable(registry.ttlSeconds(name)).orElse(900);
        final String sql = registry.readSql(name);
        final String sqlHash = shortHash(sql); // ensures cache invalidates if SQL changes

        final String scopeKey = String.format("company:%s|cluster:%s|user:%s", co, cl, u);
        final String cacheKey = String.format("dims:%s:%s:%s", name, sqlHash, scopeKey);

        return dimCache.getOrCompute(cacheKey, ttl, () -> runSmall(sql));
    }

    @DeleteMapping("{name}")
    public void evict(
        @PathVariable String name,
        @RequestParam(required = false) String companyId,
        @RequestParam(required = false) String cluster,
        @RequestParam(required = false) String userId,
        HttpServletRequest req
    ) {
        String u  = firstNonBlank(userId, header(req, "X-User-Id"), "local");
        String co = firstNonBlank(companyId, header(req, "X-Company-Id"), "default_company");
        String cl = firstNonBlank(cluster, header(req, "X-Cluster-Id"), "1");

        String sqlHash = shortHash(registry.readSql(name));
        String scopeKey = String.format("company:%s|cluster:%s|user:%s", co, cl, u);
        String cacheKey = String.format("dims:%s:%s:%s", name, sqlHash, scopeKey);

        dimCache.evict(cacheKey);
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

    private static String header(HttpServletRequest req, String name) {
        String v = req.getHeader(name);
        return (v != null && !v.isBlank()) ? v : null;
    }

    private static String firstNonBlank(String... vals) {
        for (String v : vals) if (v != null && !v.isBlank()) return v;
        return null;
    }

    private static String shortHash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            // 11-char URL-safe base64 prefix is plenty for cache segmentation
            return Base64.getUrlEncoder().withoutPadding().encodeToString(dig).substring(0, 11);
        } catch (Exception e) {
            return "nohash";
        }
    }
}
