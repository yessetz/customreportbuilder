package com.mm.customreportbuilder.model.aggrid;

public class SortModelEntry {
    private String colId;  // column field id in AG Grid
    private String sort;   // "asc" or "desc"

    public SortModelEntry() {}

    public SortModelEntry(String colId, String sort) {
        this.colId = colId;
        this.sort = sort;
    }

    public String getColId() { return colId; }
    public void setColId(String colId) { this.colId = colId; }

    public String getSort() { return sort; }
    public void setSort(String sort) { this.sort = sort; }

    public boolean isAsc() { return "asc".equalsIgnoreCase(sort); }
    public boolean isDesc() { return "desc".equalsIgnoreCase(sort); }
}
