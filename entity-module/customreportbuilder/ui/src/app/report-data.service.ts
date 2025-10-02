import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ReportDataService {
  private http = inject(HttpClient);

  async runSelectOne(): Promise<any[]> {
    const submit = await firstValueFrom(
      this.http.post<{ statementId: string }>('/api/reports/statement', { sql: 'SELECT 1 AS demo' })
    );
    const rows = await firstValueFrom(
      this.http.get<{ rows: any[] }>(`/api/reports/rows?statementId=${submit.statementId}&startRow=0&endRow=100`)
    );
    return rows.rows;
  }
}
