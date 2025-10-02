package com.mm.customreportbuilder.api;

import com.mm.customreportbuilder.service.ReportService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("api/reports")
public class ReportsController {
    private final ReportService reportService;

    public ReportsController(ReportService reportService) {
        this.reportService = reportService;
    }

    @PostMapping("/statement")
    public Map<String, Object> submit(@RequestBody Map<String, Object> body) {
        String sql = "SELECT 1";
        Object o = body.get("sql");
        if (o instanceof String s && !s.isBlank()) {
            sql = s;
        }
        return reportService.submitStatement(sql);
    }

    @GetMapping("/meta")
    public Map<String, Object> meta(@RequestParam String statementId) {
        if (statementId == null || statementId.isBlank()) {
            throw new IllegalArgumentException("statementId must not be blank");
        }
        return reportService.getStatementMeta(statementId);
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> page(@RequestParam String statementId,
                                                    @RequestParam int startRow,
                                                    @RequestParam int endRow,
                                                    @RequestParam(required = false) String sortModel,
                                                    @RequestParam(required = false) String filterModel) {
        if (statementId == null || statementId.isBlank()) {
            throw new IllegalArgumentException("statementId must not be blank");
        }
        return ResponseEntity.ok(reportService.getRows(statementId, startRow, endRow));
    }

    @DeleteMapping
    public void evict(@RequestParam String statementId) {
        reportService.evict(statementId);
    }
}