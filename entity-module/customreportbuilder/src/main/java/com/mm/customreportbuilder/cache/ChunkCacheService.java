package com.mm.customreportbuilder.cache;

import java.util.*;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;

@Service
public class ChunkCacheService {
    private final RedisTemplate<String, byte[]> bytesTemplate;
    private final RedisTemplate<String, String> stringTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final long ttlSeconds;

    public ChunkCacheService(
            RedisTemplate<String, byte[]> bytesTemplate,
            @Qualifier("redisStringTemplate") RedisTemplate<String, String> stringTemplate,
            @Value("${REDIS_CHUNK_TTL:600}") long ttlSeconds) {
        this.bytesTemplate = bytesTemplate;
        this.stringTemplate = stringTemplate;
        this.ttlSeconds = ttlSeconds;
    }

    private String metaKey(String userId, String statementId) {
        return "report:%s:%s:meta".formatted(userId, statementId);
    }

    private String chunkKey(String userId, String statementId, int index) {
        return "report:%s:%s:chunk:%d".formatted(userId, statementId, index);
    }

    public void putMeta(
        String userId,
        String statementId,
        int pageSize,
        Integer rowCount,
        List<String> columns,
        List<Map<String, Object>> schema,
        String state
    ) {
        try {
            Map<String, Object> meta = getMeta(userId, statementId);
            if (meta == null) {
                meta = new HashMap<>();
            }
            meta.put("pageSize", pageSize);
            if (rowCount != null) {
                meta.put("rowCount", rowCount);
            }
            if (columns != null) {
                meta.put("columns", columns);
            }
            if (schema != null) {
                meta.put("schema", schema);
            }
            if (state != null) {
                meta.put("state", state);
            }
            stringTemplate.opsForValue().set(metaKey(userId, statementId), mapper.writeValueAsString(meta), ttlSeconds, TimeUnit.SECONDS);

        } catch (Exception e) {
            throw new RuntimeException("Failed serialize metadata", e);
        }
    }

    public Map<String, Object> getMeta(String userId, String statementId) {
        try {
            String json = stringTemplate.opsForValue().get(metaKey(userId, statementId));
            if (json == null) {
                return null;
            }
            return mapper.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed deserialize metadata", e);
        }
    }

    // Store rows as List<List<Object>> for efficient transport
    public void putChunk(String userId, String statementId, int index, List<List<Object>> rows) {
        try {
            byte[] json = mapper.writeValueAsBytes(rows);
            byte[] gzipped = com.mm.customreportbuilder.util.GzipUtils.gzip(json);
            bytesTemplate.opsForValue().set(chunkKey(userId, statementId, index), gzipped, ttlSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed serialize chunk", e);
        }
    }

    public List<List<Object>> getChunk(String userId, String statementId, int index) {
        try {
            byte[] gzipped = bytesTemplate.opsForValue().get(chunkKey(userId, statementId, index));
            if (gzipped == null) {
                return null;
            }
            byte[] json = com.mm.customreportbuilder.util.GzipUtils.ungzip(gzipped);
            return mapper.readValue(json, new TypeReference<List<List<Object>>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed deserialize chunk", e);
        }
    }

    public void invalidateStatement(String userId, String statementId) {
        stringTemplate.delete(metaKey(userId, statementId));
        Map<String, Object> meta = getMeta(userId, statementId);
        if (meta != null) {
            Integer rowCount = (Integer) meta.get("rowCount");
            Integer pageSize = (Integer) meta.getOrDefault("pageSize", 500);
            if (rowCount != null && pageSize != null && pageSize > 0) {
                int totalChunks = (rowCount + pageSize - 1) / pageSize;
                for (int i = 0; i < totalChunks; i++) {
                    bytesTemplate.delete(chunkKey(userId, statementId, i));
                }
            }
        }
    }
}