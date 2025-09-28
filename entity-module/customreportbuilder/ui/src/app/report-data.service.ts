import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class ReportDataService {
  private baseUrl = '/api/reports';

  async submit(sql: string): Promise<any> {
    const resp = await fetch(`${this.baseUrl}/statement`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql })
    });
    if (!resp.ok) throw new Error(await resp.text());
    return resp.json();
  }

  async getMeta(statementId: string): Promise<any> {
    const resp = await fetch(`${this.baseUrl}/meta?statementId=${encodeURIComponent(statementId)}`);
    if (!resp.ok) throw new Error(await resp.text());
    return resp.json();
  }

  async getRows(statementId: string, startRow: number, endRow: number): Promise<{ rows: any[]; lastRow: number | null }> {
    const url = `${this.baseUrl}?statementId=${encodeURIComponent(statementId)}&startRow=${startRow}&endRow=${endRow}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(await resp.text());
    return resp.json();
  }

  async evict(statementId: string): Promise<void> {
    const resp = await fetch(`${this.baseUrl}?statementId=${encodeURIComponent(statementId)}`, { method: 'DELETE' });
    if (!resp.ok) throw new Error(await resp.text());
  }
}