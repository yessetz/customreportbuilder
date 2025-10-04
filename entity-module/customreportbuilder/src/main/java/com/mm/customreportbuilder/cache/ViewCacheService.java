package com.mm.customreportbuilder.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mm.customreportbuilder.util.GzipUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Caches sorted/filtered "views" derived from a base statement result.
 * Key pattern:
 *   Meta:  report:{userId}:{statementId}:view:{sig}:meta
 *   Chunk: report:{userId}:{statementId}:view:{sig}:chunk:{index}
 *
 * TTL:
 *   REDIS_VIEW_TTL (seconds) if set, otherwise REDIS_CHUNK_TTL, otherwise 600s.
 *
 * Note: This service is not wired anywhere yet (Step 2). No behavior change.
 */
@Service
public class ViewCacheService {

    private final RedisTemplate<String, byte[]> bytesTemplate;
    private final RedisTemplate<String, String> stringTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final long ttlSeconds;

    public ViewCacheService(
            RedisTemplate<String, byte[]> bytesTemplate,
            @Qualifier("redisStringTemplate") RedisTemplate<String, String> stringTemplate,
            @Value("${REDIS_VIEW_TTL:${REDIS_CHUNK_TTL:600}}") long ttlSeconds
    ) {
        this.bytesTemplate = bytesTemplate;
        this.stringTemplate = stringTemplate;
        this.ttlSeconds = ttlSeconds;
    }

    /* ====================== Keys ====================== */

    private String metaKey(String userId, String statementId, String sig) {
        return "report:%s:%s:view:%s:meta".formatted(userId, statementId, sig);
    }

    private String chunkKey(String userId, String statementId, String sig, int index) {
        return "report:%s:%s:view:%s:chunk:%d".formatted(userId, statementId, sig, index);
    }

    /* ====================== Signature ====================== */

    /**
     * Create a stable signature for (statementId, sortModelJson, filterModelJson).
     * - Canonicalizes JSON (so key order doesn’t change the sig).
     * - SHA-256, hex-encoded, 32 chars prefix (short id).
     */
    public String computeSignature(String statementId, String sortModelJson, String filterModelJson) {
        try {
            String normSort = canonicalJsonOrNull(sortModelJson);
            String normFilter = canonicalJsonOrNull(filterModelJson);
            String payload = (statementId == null ? "" : statementId) + "|" +
                    (normSort == null ? "" : normSort) + "|" +
                    (normFilter == null ? "" : normFilter);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(payload.getBytes(StandardCharsets.UTF_8));
            // return first 32 hex chars (128-bit prefix is enough)
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
                if (sb.length() >= 32) break;
            }
            return sb.toString();
        } catch (Exception e) {
            // Fallback to a simple hash if anything goes wrong
            String s = (statementId == null ? "" : statementId) + "|" +
                       (sortModelJson == null ? "" : sortModelJson) + "|" +
                       (filterModelJson == null ? "" : filterModelJson);
            return Integer.toHexString(s.hashCode());
        }
    }

    private String canonicalJsonOrNull(String json) {
        if (json == null || json.isBlank()) return null;
        try {
            // Accepts array/object; sorts object keys
            Object tree = mapper.readValue(json, Object.class);
            return mapper.writeValueAsString(tree);
        } catch (Exception e) {
            // If not valid JSON, just return trimmed original (won’t be identical if order changes)
            return json.trim();
        }
    }

    /* ====================== Meta ====================== */

    public void putMeta(String userId, String statementId, String sig,
                        int pageSize, Integer rowCount, Map<String, Object> extra) {
        try {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("type", "view");
            meta.put("baseStatementId", statementId);
            meta.put("sig", sig);
            meta.put("pageSize", pageSize);
            if (rowCount != null) meta.put("rowCount", rowCount);
            if (extra != null && !extra.isEmpty()) meta.putAll(extra);

            stringTemplate.opsForValue().set(
                    metaKey(userId, statementId, sig),
                    mapper.writeValueAsString(meta),
                    ttlSeconds, TimeUnit.SECONDS
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to put view meta", e);
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMeta(String userId, String statementId, String sig) {
        try {
            String raw = stringTemplate.opsForValue().get(metaKey(userId, statementId, sig));
            if (raw == null) return null;
            return mapper.readValue(raw, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to get view meta", e);
        }
    }

    public boolean exists(String userId, String statementId, String sig) {
        return Boolean.TRUE.equals(stringTemplate.hasKey(metaKey(userId, statementId, sig)));
    }

    /* ====================== Chunks ====================== */

    public void putChunk(String userId, String statementId, String sig, int index, List<List<Object>> rows) {
        try {
            byte[] json = mapper.writeValueAsBytes(rows);
            byte[] gz = GzipUtils.gzip(json);
            bytesTemplate.opsForValue().set(
                    chunkKey(userId, statementId, sig, index),
                    gz,
                    ttlSeconds, TimeUnit.SECONDS
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to put view chunk", e);
        }
    }

    public List<List<Object>> getChunk(String userId, String statementId, String sig, int index) {
        try {
            byte[] gz = bytesTemplate.opsForValue().get(chunkKey(userId, statementId, sig, index));
            if (gz == null) return null;
            byte[] json = GzipUtils.ungzip(gz);
            return mapper.readValue(json, new TypeReference<List<List<Object>>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to get view chunk", e);
        }
    }

    /* ====================== Eviction ====================== */

    public void invalidateView(String userId, String statementId, String sig, Integer totalChunks) {
        stringTemplate.delete(metaKey(userId, statementId, sig));
        if (totalChunks != null && totalChunks >= 0) {
            for (int i = 0; i < totalChunks; i++) {
                bytesTemplate.delete(chunkKey(userId, statementId, sig, i));
            }
        }
        // Note: We are not SCAN-ing to discover unknown chunk counts here to avoid
        // introducing SCAN/wildcard complexity. We'll track chunk counts in meta later.
    }
}
