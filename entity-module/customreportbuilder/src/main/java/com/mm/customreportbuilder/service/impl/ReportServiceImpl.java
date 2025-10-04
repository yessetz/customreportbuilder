package com.mm.customreportbuilder.service.impl;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.mm.customreportbuilder.service.ReportService;
import com.mm.customreportbuilder.databricks.DatabricksSqlClient;
import com.mm.customreportbuilder.databricks.DatabricksSqlClient.SchemaInfo;
import com.mm.customreportbuilder.cache.ChunkCacheService;
import com.mm.customreportbuilder.model.aggrid.AgGridParsedModels;
import com.mm.customreportbuilder.util.AgGridModelParser;

@Service
public class ReportServiceImpl implements ReportService {

    private static final Logger log = LoggerFactory.getLogger(ReportServiceImpl.class);

    private final DatabricksSqlClient client;
    private final ChunkCacheService cache;

    @Value("${CACHE_PAGE_SIZE:500}")
    private int PAGE_SIZE;

    @Value("${CACHE_FIRST_CHUNK_MAX_WAIT_MS:8000}")
    private long FIRST_CHUNK_MAX_WAIT_MS;

    @Value("${CACHE_FIRST_CHUNK_POLL_MS:150}")
    private long FIRST_CHUNK_POLL_MS;

    public ReportServiceImpl(DatabricksSqlClient client, ChunkCacheService cache) {
        this.client = client;
        this.cache = cache;
    }

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

                    log.debug("STORED page chunk={} rows={} statementId={} (from dbChunkIndex={})",
                            pageIdx, pageRows.size(), statementId, chunkIndex);
                }
            }
        });

        // Respond with statementId; clients can poll /meta and /rows
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

    @Override
    public Map<String, Object> getRows(String statementId, int startRow, int endRow) {
        String userId = "local";
        Map<String, Object> meta = cache.getMeta(userId, statementId);

        if (meta == null) {
            return Map.of("rows", List.of(), "lastRow", null);
        }

        int pageSize = (int) meta.getOrDefault("pageSize", PAGE_SIZE);
        Integer rowCount = (Integer) meta.get("rowCount");

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
        int toTake = endRow - startRow;
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

    // Step 3: parse & log models, then delegate to the existing method.
    // Intentionally not annotated with @Override since the interface doesn't expose this overload yet.
    public Map<String, Object> getRows(String statementId, int startRow, int endRow, String sortModelJson, String filterModelJson) {
        try {
            AgGridParsedModels parsed = AgGridModelParser.parse(sortModelJson, filterModelJson, null);
            boolean hasSort = parsed.getSortModel() != null && !parsed.getSortModel().isEmpty();
            boolean hasFilter = parsed.getFilterModel() != null && !parsed.getFilterModel().isEmpty();

            if (hasSort || hasFilter) {
                log.debug("AG Grid models received. sortModel={}, filterModel={}",
                        parsed.getCanonicalSortJson(), parsed.getCanonicalFilterJson());
            } else if ((sortModelJson != null && !sortModelJson.isBlank()) ||
                       (filterModelJson != null && !filterModelJson.isBlank())) {
                log.debug("AG Grid models ignored (invalid or empty). raw sortModel={}, filterModel={}",
                        sortModelJson, filterModelJson);
            }
        } catch (Exception e) {
            log.warn("Failed to parse AG Grid models (continuing without).", e);
        }

        return getRows(statementId, startRow, endRow);
    }

    @Override
    public void evict(String statementId) {
        cache.invalidateStatement("local", statementId);
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
}
