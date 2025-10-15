import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

type DimItem = { id: string; name: string };

const BASE = '/api/reports';

@Injectable({ providedIn: 'root' })
export class DimsDataService {
  constructor(private http: HttpClient) {}

  async loadCategories(): Promise<DimItem[]> {
    const sql = await this.readAsset('/assets/queries/dims/categories.sql');
    return await this.runSmallQuery(sql);
  }

  async loadBrands(): Promise<DimItem[]> {
    const sql = await this.readAsset('/assets/queries/dims/brands.sql');
    return await this.runSmallQuery(sql);
  }

  // ----- helpers -----
  private async readAsset(path: string): Promise<string> {
    return await firstValueFrom(this.http.get(path, { responseType: 'text' }));
  }

  private async runSmallQuery(sql: string): Promise<DimItem[]> {
    // 1) submit
    const submit = await firstValueFrom(
      this.http.post<{ statementId: string }>(`${BASE}/statement`, { sql })
    );
    const statementId = submit.statementId;

    // 2) poll meta (short)
    const meta = await this.pollMeta(statementId, 15000, 250);

    // 3) fetch rows (we expect << 10k)
    const endRow = Math.max(1000, (meta?.rowCount ?? 0) || 1000);
    const url = `${BASE}?statementId=${encodeURIComponent(statementId)}&startRow=0&endRow=${endRow}`;
    const res: any = await firstValueFrom(this.http.get(url));

    const rows = this.extractRows(res);
    return this.mapToIdName(rows);
  }

  private async pollMeta(statementId: string, timeoutMs: number, intervalMs: number): Promise<any> {
    const start = Date.now();
    while (true) {
      try {
        const meta = await firstValueFrom(this.http.get(`${BASE}/meta?statementId=${statementId}`));
        const mc: any = meta;
        const state = String(mc?.state || '').toUpperCase();
        if (typeof mc?.rowCount === 'number') return mc;
        if (['SUCCEEDED', 'DONE', 'COMPLETED'].includes(state)) return mc;
      } catch {}
      if (Date.now() - start > timeoutMs) throw new Error('Timed out loading dim data.');
      await new Promise(r => setTimeout(r, intervalMs));
    }
  }

  // Tolerant extractors (array-of-arrays or array-of-objects)
  private extractRows(res: any): any[] {
    if (Array.isArray(res?.rows)) return res.rows;
    if (Array.isArray(res?.data)) return res.data;
    if (Array.isArray(res?.result)) return res.result;
    if (Array.isArray(res)) return res;
    if (Array.isArray(res?.chunks)) {
      for (const ch of res.chunks) {
        const rows = ch?.rows || ch?.data || ch?.result;
        if (Array.isArray(rows) && rows.length) return rows;
      }
    }
    return [];
  }

  private mapToIdName(raw: any[]): DimItem[] {
    if (!raw?.length) return [];
    const first = raw[0];
    // If rows are objects with id/name
    if (first && typeof first === 'object' && !Array.isArray(first) && ('id' in first) && ('name' in first)) {
      return raw.map((r: any) => ({ id: String(r.id), name: String(r.name) }));
    }
    // Else rows are arrays -> assume two columns [id, name]
    return raw.map((r: any[]) => ({ id: String(r?.[0] ?? ''), name: String(r?.[1] ?? '') }))
              .filter(r => r.id && r.name);
  }
}
