package com.mm.customreportbuilder.service.impl;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;
import com.mm.customreportbuilder.service.ReportService;
import com.mm.customreportbuilder.databricks.DatabricksSqlClient;
import com.mm.customreportbuilder.cache.ChunkCacheService;

@Service
public class ReportServiceImpl implements ReportService {

    private static final Logger log = LoggerFactory.getLogger(ReportServiceImpl.class);

    private final DatabricksSqlClient client;
    private final ChunkCacheService cache;

    @Value("${CACHE_PAGE_SIZE:500}")
    private int PAGE_SIZE;

    public ReportServiceImpl(DatabricksSqlClient client, ChunkCacheService cache) {
        this.client = client;
        this.cache = cache;
    }

    @Override
    public Map<String, Object> submitStatement(String sql) {
        String userId = "local";
        String statementId = client.submitStatement(sql);
        DatabricksSqlClient.SchemaInfo schemaInfo = client.getSchema(statementId);
        cache.putMeta(userId, statementId, PAGE_SIZE, null, schemaInfo.columnNames(), schemaInfo.columnMeta(), "PENDING");

        client.streamChunks(statementId, PAGE_SIZE, (chunkIndex, rows, totalRows, state) -> {
            if (chunkIndex >= 0 && rows != null && !rows.isEmpty()) {
                cache.putChunk(userId, statementId, chunkIndex, rows);
                log.debug("STORED chunk={} rows={} statementId={}", chunkIndex, rows.size(), statementId);
            }
            if (totalRows != null || state != null) {
                cache.putMeta(userId, statementId, PAGE_SIZE, totalRows, null, null, state);
            }
        });

        return Map.of(
            "statementId", statementId,
            "state", "PENDING",
            "columns", schemaInfo.columnNames(),
            "schema", schemaInfo.columnMeta()
        );
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

        if (endRow <= startRow)
            return Map.of("rows", List.of(), "lastRow", rowCount);

        int firstChunk = Math.max(0, startRow / pageSize);
        int lastChunk = Math.max(firstChunk, (endRow - 1) / pageSize);
        List<List<Object>> buffer = new ArrayList<>();

        for (int index = firstChunk; index <= lastChunk; index++) {
            List<List<Object>> chunk = cache.getChunk(userId, statementId, index);
            if (chunk != null && !chunk.isEmpty()) {
                buffer.addAll(chunk);
            } else {
                break;
            }
        }

        int offset = startRow - (firstChunk * pageSize);
        int toTake = endRow - startRow;
        List<List<Object>> page;

        if (offset < 0 || buffer.isEmpty()) {
            page = List.of();
        } else {
            int from = Math.min(offset, buffer.size());
            int to = Math.min(from + toTake, buffer.size());
            page = buffer.subList(from, to);
        }

        return Map.of("rows", page, "lastRow", rowCount);
    }

    @Override
    public void evict(String statementId) {
        cache.invalidateStatement("local", statementId);
    }
}