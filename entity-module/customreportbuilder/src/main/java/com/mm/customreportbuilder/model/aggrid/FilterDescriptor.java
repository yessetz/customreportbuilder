package com.mm.customreportbuilder.model.aggrid;

import java.util.List;

/**
 * Matches AG Grid filter model shape (generic for text/number/date).
 * See: https://www.ag-grid.com/javascript-data-grid/filter-set-api/
 */
public class FilterDescriptor {
    // common
    private String filterType; // "text" | "number" | "date" | etc.
    private String type;       // operation, e.g. "contains", "equals", "greaterThan", "inRange", ...
    private String filter;     // value (text/number)
    private String filterTo;   // used for inRange for number
    private String dateFrom;   // used for date
    private String dateTo;     // used for date range
    private String operator;   // "AND" | "OR" (compound)
    private List<FilterDescriptor> conditions; // compound filters

    public boolean isCompound() {
        return operator != null && conditions != null && !conditions.isEmpty();
    }

    public String getFilterType() { return filterType; }
    public void setFilterType(String filterType) { this.filterType = filterType; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getFilter() { return filter; }
    public void setFilter(String filter) { this.filter = filter; }

    public String getFilterTo() { return filterTo; }
    public void setFilterTo(String filterTo) { this.filterTo = filterTo; }

    public String getDateFrom() { return dateFrom; }
    public void setDateFrom(String dateFrom) { this.dateFrom = dateFrom; }

    public String getDateTo() { return dateTo; }
    public void setDateTo(String dateTo) { this.dateTo = dateTo; }

    public String getOperator() { return operator; }
    public void setOperator(String operator) { this.operator = operator; }

    public List<FilterDescriptor> getConditions() { return conditions; }
    public void setConditions(List<FilterDescriptor> conditions) { this.conditions = conditions; }
}
