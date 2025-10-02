package com.mm.customreportbuilder.databricks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

@Component
public class DatabricksSqlClient {
    private static final Logger log = LoggerFactory.getLogger(DatabricksSqlClient.class);

    private final RestTemplate rest;
    private final String host;
    private final String token;
    private final String warehouseId;
    private final ExecutorService exec = Executors.newCachedThreadPool();
    private final ObjectMapper mapper = new ObjectMapper();

    public DatabricksSqlClient(
            @Value("${DATABRICKS_HOST:}") String host,
            @Value("${DATABRICKS_TOKEN:}") String token,
            @Value("${DATABRICKS_WAREHOUSEID:}") String warehouseId,
            @Value("${DATABRICKS_TIMEOUT_MS:3000}") long connectTimeoutMs,
            @Value("${DATABRICKS_READTIMEOUT_MS:30000}") long readTimeoutMs) {

        String h = (host == null) ? "" : host.trim();
        if (h.endsWith("/")) h = h.substring(0, h.length() - 1);
        this.host = h;
        this.token = (token == null) ? "" : token.trim();
        this.warehouseId = (warehouseId == null) ? "" : warehouseId.trim();

        org.springframework.web.util.DefaultUriBuilderFactory f = new org.springframework.web.util.DefaultUriBuilderFactory();
        f.setEncodingMode(org.springframework.web.util.DefaultUriBuilderFactory.EncodingMode.NONE);

        this.rest = new RestTemplateBuilder()
                .uriTemplateHandler(f)
                .setConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .setReadTimeout(Duration.ofMillis(readTimeoutMs))
                .build();
    }

    @PostConstruct
    void validate() {
        List<String> missing = new ArrayList<>();
        if (host.isBlank()) missing.add("databricks.host");
        if (token.isBlank()) missing.add("databricks.token");
        if (warehouseId.isBlank()) missing.add("databricks.warehouseId");
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing required Databricks configuration: " + String.join(", ", missing));
        }
    }

    public String submitStatement(String sql) {
        Map<String, Object> body = new HashMap<>();
        body.put("statement", sql);
        body.put("warehouse_id", warehouseId);
        body.put("disposition", "EXTERNAL_LINKS");
        body.put("format", "JSON_ARRAY");

        Map<?, ?> resp = exchange("/api/2.0/sql/statements/", HttpMethod.POST, body, Map.class);
        Object id = resp == null ? null : resp.get("statement_id");
        if (!(id instanceof String)) {
            throw new IllegalStateException("Databricks did not return statement_id");
        }
        log.debug("Submitted statement id={} disposition=EXTERNAL_LINKS format=JSON_ARRAY", id);
        return (String) id;
    }

    public record SchemaInfo(List<String> columnNames, List<Map<String, Object>> columnMeta) {}

    public SchemaInfo getSchema(String statementId) {
        Map<String, Object> status = fetchStatus(statementId);
        if (status == null) return new SchemaInfo(List.of(), List.of());
        Map<String, Object> manifest = cast(status.get("manifest"));
        Map<String, Object> result = cast(status.get("result"));
        Map<String, Object> schema = null;

        if (manifest != null) schema = cast(manifest.get("schema"));
        if (schema == null && result != null) schema = cast(result.get("schema"));
        if (schema == null) return new SchemaInfo(List.of(), List.of());

        List<Map<String, Object>> cols = cast(schema.get("columns"));
        if (cols == null) return new SchemaInfo(List.of(), List.of());
        cols.sort(Comparator.comparingInt(c -> ((Number) c.getOrDefault("position", 0)).intValue()));

        List<String> names = new ArrayList<>();
        for (Map<String, Object> c : cols) {
            Object n = c.get("name");
            if (n instanceof String s) names.add(s);
        }
        return new SchemaInfo(names, cols);
    }

    public void streamChunks(String statementId, int pageSize, ChunkListener listener) {
        exec.submit(() -> {
            try {
                boolean processed = false;
                int pollMs = 1000;
                while (true) {
                    Map<String, Object> status = fetchStatus(statementId);
                    if (status == null) {
                        log.warn("Null status for statement {}", statementId);
                        Thread.sleep(pollMs);
                        continue;
                    }
                    String state = extractState(status);
                    Map<String, Object> manifest = cast(status.get("manifest"));
                    Integer totalRows = manifest != null ? asInt(manifest.get("total_row_count")) : null;
                    Integer totalChunkCount = manifest != null ? asInt(manifest.get("total_chunk_count")) : null;
                    listener.onChunk(-1, List.of(), totalRows, state);

                    if (isTerminal(state)) {
                        if (!processed) {
                            processed = true;
                            Map<String, Object> result = cast(status.get("result"));
                            List<Map<String, Object>> externalLinks = extractExternalLinks(result);
                            if (!externalLinks.isEmpty()) {
                                for (int i = 0; i < externalLinks.size(); i++) {
                                    Map<String, Object> link = externalLinks.get(i);
                                    Integer idxOpt = asInt(link.get("chunk_index"));
                                    int chunkIdx = (idxOpt != null) ? idxOpt : i;
                                    String url = (String) link.get("external_link");
                                    if (url == null || url.isBlank()) continue;
                                    List<List<Object>> rows = downloadExternalLink(url, chunkIdx);
                                    if (!rows.isEmpty()) {
                                        listener.onChunk(chunkIdx, rows, totalRows, state);
                                    }
                                }
                            } else {
                                int count = (totalChunkCount != null && totalChunkCount > 0) ? totalChunkCount : 1;
                                for (int i = 0; i < count; i++) {
                                    List<List<Object>> rows = fetchChunk(statementId, i, pageSize);
                                    if (!rows.isEmpty()) {
                                        listener.onChunk(i, rows, totalRows, state);
                                    }
                                }
                            }
                        }
                        break;
                    }
                    Thread.sleep(pollMs);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Streaming interrupted statementId={}", statementId);
            } catch (Exception e) {
                log.error("Error streaming chunks for statementId={}", statementId, e);
            }
        });
    }

    private String extractState(Map<String, Object> status) {
        if (status == null) return null;
        Map<String, Object> s = cast(status.get("status"));
        if (s != null && s.get("state") instanceof String st) return st;
        if (status.get("state") instanceof String st2) return st2;
        return null;
    }

    private List<Map<String, Object>> extractExternalLinks(Map<String, Object> result) {
        if (result == null) return List.of();
        Object o = result.get("external_links");
        if (!(o instanceof List<?> list)) return List.of();
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object e : list) {
            if (e instanceof Map<?, ?> m && m.get("external_link") instanceof String) {
                out.add(cast(m));
            }
        }
        return out;
    }

    private List<List<Object>> downloadExternalLink(String url, int chunkIdx) {
        try {
            HttpHeaders h = new HttpHeaders();
            h.setAccept(List.of(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN));
            URI uri = URI.create(url);
            ResponseEntity<byte[]> resp = rest.exchange(uri, HttpMethod.GET, new HttpEntity<>(h), byte[].class);
            if (!resp.getStatusCode().is2xxSuccessful() || resp.getBody() == null) {
                log.warn("External link non-200 chunk={} status={}", chunkIdx, resp.getStatusCode());
                return List.of();
            }
            byte[] payload = resp.getBody();
            if (isGzip(payload)) {
                try (GZIPInputStream gis = new GZIPInputStream(new java.io.ByteArrayInputStream(payload))) {
                    payload = gis.readAllBytes();
                }
            }
            String body = new String(payload, java.nio.charset.StandardCharsets.UTF_8).trim();
            List<List<Object>> rows;
            if (body.startsWith("[")) {
                try {
                    rows = mapper.readValue(body, new TypeReference<List<List<Object>>>(){});
                } catch (Exception ex) {
                    List<Map<String, Object>> objs = mapper.readValue(body, new TypeReference<List<Map<String, Object>>>(){});
                    rows = coerceObjectsToArrays(objs);
                }
            } else {
                log.warn("External link unexpected content chunk={} prefix={}", chunkIdx, body.substring(0, Math.min(40, body.length())));
                rows = List.of();
            }
            log.debug("Downloaded external link chunk={} rows={}", chunkIdx, rows.size());
            return rows;
        } catch (HttpStatusCodeException e) {
            log.error("External Link HTTP {} chunk={} url={} body={}", e.getStatusCode(), chunkIdx, url, e.getResponseBodyAsString());
            return List.of();
        } catch (Exception e) {
            log.error("Error downloading external link chunk={} url={}", chunkIdx, url, e);
            return List.of();
        }
    }

    private boolean isGzip(byte[] data) {
        return data != null && data.length >= 2 && (data[0] == (byte) 0x1f) && (data[1] == (byte) 0x8b);
    }

    private List<List<Object>> coerceObjectsToArrays(List<Map<String, Object>> objects) {
        if (objects == null || objects.isEmpty()) return List.of();
        List<String> keys = new ArrayList<>(objects.get(0).keySet());
        List<List<Object>> out = new ArrayList<>(objects.size());
        for (Map<String, Object> m : objects) {
            List<Object> row = new ArrayList<>(keys.size());
            for (String k : keys) {
                row.add(m.get(k));
            }
            out.add(row);
        }
        return out;
    }

    public List<List<Object>> fetchChunk(String statementId, int chunkIndex, int pageSize) {
        String path = "/api/2.0/sql/statements/" + statementId + "/result/chunks/" + chunkIndex
                + "?row_limit=" + pageSize + "&format=JSON_ARRAY";
        Map<?, ?> resp = exchange(path, HttpMethod.GET, null, Map.class);
        if (resp == null) return List.of();

        List<List<Object>> rows = null;

        Object chunkObj = resp.get("chunk");
        if (chunkObj instanceof Map<?, ?>) {
            Map<String, Object> chunk = cast(chunkObj);
            if (chunk.get("rows") instanceof List<?>) rows = cast(chunk.get("rows"));
            if ((rows == null || rows.isEmpty()) && chunk.get("data_array") instanceof List<?>)
                rows = cast(chunk.get("data_array"));
            if ((rows == null || rows.isEmpty()) && chunk.get("external_link") instanceof String link)
                rows = downloadExternalLink(link, chunkIndex);
        }

        if ((rows == null || rows.isEmpty()) && resp.get("data_array") instanceof List<?>)
            rows = cast(resp.get("data_array"));
        if ((rows == null || rows.isEmpty()) && resp.get("rows") instanceof List<?>)
            rows = cast(resp.get("rows"));
        if ((rows == null || rows.isEmpty()) && resp.get("external_link") instanceof String link)
            rows = downloadExternalLink(link, chunkIndex);
        if ((rows == null || rows.isEmpty()) && resp.get("external_links") instanceof List<?> extList) {
            for (Object o : extList) {
                if (o instanceof Map<?, ?> m && m.get("external_link") instanceof String link2) {
                    rows = downloadExternalLink((String) link2, chunkIndex);
                    if (rows != null && !rows.isEmpty()) break;
                }
            }
        }

        if (rows == null) rows = List.of();
        log.debug("Fetched chunk endpoint chunkIndex={} rows={}", chunkIndex, rows.size());
        return rows;
    }

    private Map<String, Object> fetchStatus(String statementId) {
        return exchange("/api/2.0/sql/statements/" + statementId, HttpMethod.GET, null, Map.class);
    }

    private Integer asInt(Object o) {
        if (o instanceof Number n) return n.intValue();
        if (o instanceof String s) {
            try { return Integer.parseInt(s); } catch (NumberFormatException ignored) {}
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> T cast(Object o) {
        return (T) o;
    }

    private <T> T exchange(String path, HttpMethod method, Object body, Class<T> type) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        HttpEntity<?> entity = new HttpEntity<>(body, headers);
        try {
            ResponseEntity<T> resp = rest.exchange(host + path, method, entity, type);
            return resp.getBody();
        } catch (HttpClientErrorException e) {
            log.error("Databricks client error {} path={} body={}", e.getStatusCode(), path, e.getResponseBodyAsString());
            throw new IllegalStateException("Databricks client error " + e.getStatusCode(), e);
        } catch (HttpServerErrorException e) {
            log.error("Databricks server error {} path={} body={}", e.getStatusCode(), path, e.getResponseBodyAsString());
            throw new IllegalStateException("Databricks server error " + e.getStatusCode(), e);
        } catch (ResourceAccessException e) {
            log.error("Databricks resource access error path={}", path, e);
            throw new IllegalStateException("Databricks resource access error: " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    public interface ChunkListener {
        void onChunk(int chunkIndex, List<List<Object>> rows, Integer totalRows, String state);
    }
}