import { Component, inject } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { AgGridAngular } from 'ag-grid-angular';
import type { ColDef, GridReadyEvent } from 'ag-grid-community';
import { themeQuartz } from 'ag-grid-community';

const BASE = '/api/reports'; // no trailing slash

type Meta = {
  state?: string;           // e.g., "SUCCEEDED"
  rowCount?: number;        // total rows
  pageSize?: number;
  columns?: string[];       // e.g., ["demo"]
  schema?: Array<{ name: string; type_text?: string; type_name?: string; position?: number }>;
  status?: { state?: string; message?: string; };
};

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [AgGridAngular],
  template: `
    <div style="padding:12px;">
      <h3>Databricks → Redis → API → Grid (no styles)</h3>

      <button (click)="run()" [disabled]="loading">
        {{ loading ? 'Running…' : 'Run SELECT 1 AS demo' }}
      </button>

      <p *ngIf="error" style="color:#b00; white-space:pre-wrap; margin-top:8px;">
        {{ error }}
      </p>

      <div *ngIf="statementId" style="margin-top:12px;">
        <strong>statementId</strong>
        <pre>{{ statementId }}</pre>
      </div>

      <div *ngIf="meta" style="margin:12px 0;">
        <strong>meta</strong>
        <pre>{{ meta | json }}</pre>
      </div>

      <div style="height:400px; width:100%; margin-top:12px;">
        <ag-grid-angular
          [theme]="theme"
          [columnDefs]="columnDefs"
          [defaultColDef]="defaultColDef"
          [rowData]="rowData"
          (gridReady)="onGridReady($event)"
          style="color: black; width: 100%; height: 700px;">
        </ag-grid-angular>
      </div>

      <div *ngIf="debugResponse" style="margin-top:12px;">
        <strong>raw rows response (debug)</strong>
        <pre>{{ debugResponse | json }}</pre>
      </div>
    </div>
  `,
})
export class AppComponent {
  private http = inject(HttpClient);

  theme = themeQuartz; // Theming API only (no CSS files)
  loading = false;
  error = '';
  statementId: string | null = null;
  meta: Meta | null = null;

  columnDefs: ColDef[] = [];
  defaultColDef: ColDef = { resizable: true, sortable: true, filter: true, flex: 1, minWidth: 120 };
  rowData: any[] = [];

  debugResponse: any = null;

  onGridReady(_e: GridReadyEvent) {
    // nothing needed; we bind [rowData]
  }

  async run() {
    this.loading = true;
    this.error = '';
    this.rowData = [];
    this.columnDefs = [];
    let querySQL = 'SELECT 1 AS demo';
    try {
      // 1) submit
      const submit = await firstValueFrom(
        this.http.post<{ statementId: string }>(`${BASE}/statement`, { sql: querySQL })
      );
      this.statementId = submit.statementId;
      // 2) poll meta
      this.meta = await this.pollMeta(this.statementId, 20000, 300);
      const rc = Math.max(this.meta.rowCount ?? 0, 0);
      const endExclusive = Math.max(1, rc); 
      const url = this.mkRowsUrl(this.statementId, 0, endExclusive);
      
      let res = await this.safeGet<any>(url);
      let rawRows = this.extractRows(res);

      if (rawRows.length === 0 && rc > 0) {
        // Try again if we expected rows but got none
        await this.delay(500);
        res = await this.safeGet<any>(url);
        rawRows = this.extractRows(res);
      }
      
      this.debugResponse = res;

      const colNames: string[] = 
        (this.meta?.columns && this.meta.columns.length 
          ? this.meta.columns 
          : this.meta?.schema?.length
            ? [...this.meta.schema]
              .sort((a, b) => (a.position ?? 0) - (b.position ?? 0))
              .map(c => c.name)
            : rawRows.length
              ? rawRows[0].map((_v: any, i: number) => 'col' + i)
              : []);
      
      const objects = Array.isArray(rawRows)
          ? rawRows.map(r => {
              if (!Array.isArray(r)) return r; // already an object
              const o: any = {};
              for (let i = 0; i < colNames.length; i++) {
                o[colNames[i]] = r[i];
              }
              return o;
            })
          : [];
    
      this.columnDefs = colNames.map((name: string): ColDef => ({ headerName: name, field: name }));
      this.rowData = objects;
    } catch (err) {
      this.error = this.normalizeHttpError(err);
      console.error('run() failed:', err);
    } finally {
      this.loading = false;
    }
  }

  private mkRowsUrl(statementId: string, start: number, end: number) {
    const p = new URLSearchParams();
    p.set('statementId', statementId);
    p.set('startRow', String(start));
    p.set('endRow', String(end));
    return `${BASE}?${p.toString()}`;
  }

  private async safeGet<T = any>(url: string): Promise<T> {
    return await firstValueFrom(this.http.get<T>(url));
  }

  private async pollMeta(statementId: string, timeoutMs: number, intervalMs: number): Promise<Meta> {
    const start = Date.now();
    while (true) {
      let meta: Meta;
      try {
        meta = await firstValueFrom(this.http.get<Meta>(`${BASE}/statement?statementId=${statementId}`));
      } catch {
        if (Date.now() - start > timeoutMs) throw new Error('Timed out waiting for statement meta.');
        await this.delay(intervalMs);
        continue;
      }
      const state = (meta.state ?? '').toUpperCase();
      if (typeof meta.rowCount === 'number') return meta; // treat counts as "ready" too
      if (['SUCCEEDED', 'DONE', 'COMPLETED'].includes(state)) return meta;
      if (Date.now() - start > timeoutMs) throw new Error('Timed out waiting for statement meta.');
      await this.delay(intervalMs);
    }
  }

  private extractRows(res: any): any[] {
    // Try common shapes first
    if (Array.isArray(res?.rows)) return res.rows;
    if (Array.isArray(res?.data)) return res.data;
    if (Array.isArray(res?.result)) return res.result;
    if (Array.isArray(res)) return res;

    // Some APIs return chunks; pick first non-empty chunk
    if (Array.isArray(res?.chunks)) {
      for (const ch of res.chunks) {
        const rows = ch?.rows || ch?.data || ch?.result;
        if (Array.isArray(rows) && rows.length) return rows;
      }
    }
    // Last resort: null/undefined -> []
    return [];
  }

  private delay(ms: number) {
    return new Promise(res => setTimeout(res, ms));
  }

  private normalizeHttpError(err: unknown): string {
    if (err instanceof HttpErrorResponse) {
      const body = typeof err.error === 'string' ? err.error : JSON.stringify(err.error);
      return `${err.status} ${err.statusText} — ${err.url}\n${body}`;
    }
    if (err instanceof Error) return err.message;
    try { return JSON.stringify(err); } catch { return String(err); }
  }
}
