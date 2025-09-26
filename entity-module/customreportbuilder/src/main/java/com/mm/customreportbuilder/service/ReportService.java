package com.mm.customreportbuilder.service;

import java.util.Map;

public interface ReportService {
    Map<String, Object> submitStatement(String sql);
    Map<String, Object> getStatementMeta(String statementId);
    Map<String, Object> getRows(String statementId, int startRow, int endRow);
    void evict(String statementId);
}