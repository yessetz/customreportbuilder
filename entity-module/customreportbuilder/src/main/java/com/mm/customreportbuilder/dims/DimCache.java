package com.mm.customreportbuilder.dims;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Component
public class DimCache {
  private static final class Entry {
    final List<Map<String,String>> value;
    final long expiresAtMs;
    Entry(List<Map<String,String>> value, long expiresAtMs) { this.value = value; this.expiresAtMs = expiresAtMs; }
  }

  private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

  public List<Map<String,String>> get(String key) {
    Entry e = store.get(key);
    if (e == null) return null;
    if (System.currentTimeMillis() >= e.expiresAtMs) {
      store.remove(key, e);
      return null;
    }
    return e.value;
  }

  public List<Map<String,String>> getOrCompute(String key, int ttlSeconds, Supplier<List<Map<String,String>>> supplier) {
    List<Map<String,String>> cached = get(key);
    if (cached != null) return cached;
    List<Map<String,String>> val = supplier.get();
    put(key, val, ttlSeconds);
    return val;
  }

  public void put(String key, List<Map<String,String>> value, int ttlSeconds) {
    long exp = System.currentTimeMillis() + Math.max(1, ttlSeconds) * 1000L;
    store.put(key, new Entry(value, exp));
  }

  public void evict(String key) { store.remove(key); }
  public void clear() { store.clear(); }
}
