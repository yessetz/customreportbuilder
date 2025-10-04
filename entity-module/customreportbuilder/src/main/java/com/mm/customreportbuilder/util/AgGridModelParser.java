package com.mm.customreportbuilder.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mm.customreportbuilder.model.aggrid.AgGridParsedModels;
import com.mm.customreportbuilder.model.aggrid.FilterDescriptor;
import com.mm.customreportbuilder.model.aggrid.SortModelEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Parses AG Grid sortModel/filterModel JSON strings into POJOs and performs light validation:
 * - unknown/empty models -> empty
 * - invalid shapes -> empty with note
 * - optional "allowed columnIds" check (if provided)
 *
 * No exceptions are thrown for invalid inputs (to keep Step 3 behavior unchanged).
 */
public final class AgGridModelParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AgGridModelParser() {}

    public static AgGridParsedModels parse(String sortModelJson,
                                           String filterModelJson,
                                           Set<String> allowedColumnIds /* nullable */) {
        List<SortModelEntry> sortModel = parseSort(sortModelJson);
        Map<String, FilterDescriptor> filterModel = parseFilter(filterModelJson);

        // Light validation: remove entries whose colId is not in allowed set (if provided)
        if (allowedColumnIds != null && !allowedColumnIds.isEmpty()) {
            sortModel = sortModel.stream()
                    .filter(s -> s.getColId() != null && allowedColumnIds.contains(s.getColId()))
                    .collect(Collectors.toList());

            filterModel = filterModel.entrySet().stream()
                    .filter(e -> e.getKey() != null && allowedColumnIds.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a,b) -> a, LinkedHashMap::new));
        }

        // Canonical JSON (stable field order)
        String canonicalSort = writeJsonQuietly(sortModel);
        String canonicalFilter = writeJsonQuietly(filterModel);

        return new AgGridParsedModels(sortModel, filterModel, canonicalSort, canonicalFilter);
    }

    public static List<SortModelEntry> parseSort(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            List<SortModelEntry> list = MAPPER.readValue(json, new TypeReference<List<SortModelEntry>>() {});
            if (list == null) return Collections.emptyList();
            // Keep only asc/desc
            return list.stream()
                    .filter(x -> x.getColId() != null && x.getSort() != null)
                    .filter(x -> {
                        String s = x.getSort().toLowerCase(Locale.ROOT);
                        return "asc".equals(s) || "desc".equals(s);
                    })
                    .collect(Collectors.toList());
        } catch (Exception ignore) {
            return Collections.emptyList();
        }
    }

    public static Map<String, FilterDescriptor> parseFilter(String json) {
        if (json == null || json.isBlank()) return Collections.emptyMap();
        try {
            Map<String, FilterDescriptor> map = MAPPER.readValue(json, new TypeReference<Map<String, FilterDescriptor>>() {});
            return map == null ? Collections.emptyMap() : map;
        } catch (Exception ignore) {
            return Collections.emptyMap();
        }
    }

    private static String writeJsonQuietly(Object o) {
        try { return MAPPER.writeValueAsString(o); }
        catch (Exception e) { return null; }
    }
}
