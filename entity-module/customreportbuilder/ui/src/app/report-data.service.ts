// src/app/report-data.service.ts
import { inject, Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ReportDataService {
  private http = inject(HttpClient);

  async runSelectOne() {
    // 1) submit
    const submit = await firstValueFrom(
      this.http.post<{ statementId: string }>(
        '/api/reports/submit',
        { sql: 'SELECT 1 AS demo' } // or via query param depending on your controller
      )
    );

    // 2) (optional) poll meta until SUCCEEDED, then:
    // 3) fetch first page of rows
    const rows = await firstValueFrom(
      this.http.get<{ rows: any[] }>(
        `/api/reports/rows?statementId=${submit.statementId}&startRow=0&endRow=100`
      )
    );

    return rows.rows; // shape like [{ demo: 1 }]
  }
}
