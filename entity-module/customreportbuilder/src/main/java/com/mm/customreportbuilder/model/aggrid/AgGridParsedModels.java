package com.mm.customreportbuilder.model.aggrid;

import java.util.List;
import java.util.Map;

/** Container for parsed models + canonical JSON strings */
public class AgGridParsedModels {
    private List<SortModelEntry> sortModel;
    private Map<String, FilterDescriptor> filterModel;
    private String canonicalSortJson;   // normalized JSON (stable order)
    private String canonicalFilterJson; // normalized JSON

    public AgGridParsedModels() {}

    public AgGridParsedModels(List<SortModelEntry> sortModel,
                              Map<String, FilterDescriptor> filterModel,
                              String canonicalSortJson,
                              String canonicalFilterJson) {
        this.sortModel = sortModel;
        this.filterModel = filterModel;
        this.canonicalSortJson = canonicalSortJson;
        this.canonicalFilterJson = canonicalFilterJson;
    }

    public List<SortModelEntry> getSortModel() { return sortModel; }
    public void setSortModel(List<SortModelEntry> sortModel) { this.sortModel = sortModel; }

    public Map<String, FilterDescriptor> getFilterModel() { return filterModel; }
    public void setFilterModel(Map<String, FilterDescriptor> filterModel) { this.filterModel = filterModel; }

    public String getCanonicalSortJson() { return canonicalSortJson; }
    public void setCanonicalSortJson(String canonicalSortJson) { this.canonicalSortJson = canonicalSortJson; }

    public String getCanonicalFilterJson() { return canonicalFilterJson; }
    public void setCanonicalFilterJson(String canonicalFilterJson) { this.canonicalFilterJson = canonicalFilterJson; }
}
