package com.mm.customreportbuilder.dims;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Component
public class DimCache {

  private final StringRedisTemplate redis;
  private final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<List<Map<String, String>>> LIST_OF_MAP =
      new TypeReference<>() {};

  public DimCache(StringRedisTemplate redis) {
    this.redis = redis;
  }

  public List<Map<String,String>> get(String key) {
    String json = redis.opsForValue().get(key);
    if (json == null) return null;
    try {
      return mapper.readValue(json, LIST_OF_MAP);
    } catch (Exception e) {
      // Corrupt entry? evict and behave like a miss.
      redis.delete(key);
      return null;
    }
  }

  public List<Map<String,String>> getOrCompute(
      String key, int ttlSeconds, java.util.function.Supplier<List<Map<String,String>>> supplier) {
    List<Map<String,String>> cached = get(key);
    if (cached != null) return cached;
    List<Map<String,String>> val = supplier.get();
    put(key, val, ttlSeconds);
    return val;
  }

  public void put(String key, List<Map<String,String>> value, int ttlSeconds) {
    try {
      String json = mapper.writeValueAsString(value);
      redis.opsForValue().set(key, json, Duration.ofSeconds(Math.max(1, ttlSeconds)));
    } catch (Exception e) {
      // Non-fatal: just skip caching if serialization fails
    }
  }

  public void evict(String key) {
    redis.delete(key);
  }

  public void clear() {
    // Be careful in prod. Keeping noop here; use a prefix scan if needed.
  }
}
