import { Component, inject, OnInit } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { AgGridAngular } from 'ag-grid-angular';
import type { ColDef, GridReadyEvent } from 'ag-grid-community';
import { themeQuartz } from 'ag-grid-community';

const BASE = '/api/reports'; // no trailing slash
const STORAGE_KEY = 'crb:lastStatementId';

type Meta = {
  state?: string;           // e.g., "SUCCEEDED"
  rowCount?: number;        // total rows
  pageSize?: number;        // server page size (default 500)
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

      <div style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">
        <button (click)="run()" [disabled]="loading">
          {{ loading ? 'Running…' : 'Run SELECT 1 AS demo' }}
        </button>

        <button *ngIf="statementId" (click)="clearResume()" [disabled]="loading" title="Forget cached result">
          Clear resume
        </button>

        <span *ngIf="resumeNotice && !loading" style="font-size:12px; opacity:0.8;">{{ resumeNotice }}</span>
      </div>

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

      <div style="border:1px solid #ddd; padding:8px;">
        <ag-grid-angular
          [theme]="theme.quartz"
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
export class AppComponent implements OnInit {
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
  resumeNotice = '';

  // ---------- Lifecycle: resume on reload ----------
  async ngOnInit() {
    const last = localStorage.getItem(STORAGE_KEY);
    if (!last) return;

    // Try to restore if Redis still has meta
    try {
      this.loading = true;
      this.resumeNotice = 'Resuming last result…';
      this.statementId = last;

      // Quick poll for meta (shorter window than initial submit)
      this.meta = await this.pollMeta(this.statementId, /*timeout*/ 5000, /*interval*/ 250);

      // If meta exists, fetch initial block [0 .. rowCount) or [0..1) as before
      await this.loadInitialRowsForCurrentStatement();

      this.resumeNotice = 'Resumed from previous result.';
    } catch (_e) {
      // Meta likely expired from Redis; forget the cached key silently
      localStorage.removeItem(STORAGE_KEY);
      this.statementId = null;
      this.meta = null;
      this.resumeNotice = '';
    } finally {
      this.loading = false;
    }
  }

  // ---------- UI actions ----------
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

      // Save for resume
      localStorage.setItem(STORAGE_KEY, this.statementId);

      // 2) poll meta
      this.meta = await this.pollMeta(this.statementId, /*timeout*/ 20000, /*interval*/ 300);

      // 3) load initial rows
      await this.loadInitialRowsForCurrentStatement();

    } catch (err) {
      this.error = this.normalizeHttpError(err);
      console.error('run() failed:', err);
    } finally {
      this.loading = false;
    }
  }

  async clearResume() {
    if (!this.statementId) return;

    try {
      await firstValueFrom(this.http.delete<void>(`${BASE}?statementId=${this.statementId}`));
    } catch {
      // Even if server evict fails (expired already), still clear local
    } finally {
      localStorage.removeItem(STORAGE_KEY);
      this.statementId = null;
      this.meta = null;
      this.rowData = [];
      this.columnDefs = [];
      this.resumeNotice = '';
    }
  }

  // ---------- Helpers ----------
  private async loadInitialRowsForCurrentStatement() {
    const rc = Math.max(this.meta?.rowCount ?? 0, 0);
    // Original behavior: if rowCount is 0 or unknown, at least ask for [0..1)
    const endExclusive = Math.max(1, rc);

    const url = this.mkRowsUrl(this.statementId!, 0, endExclusive);
    const res = await this.safeGet<any>(url);
    this.debugResponse = res;

    const rawRows = this.extractRows(res);

    // Decide columns: meta.columns > meta.schema (sorted by position) > sample row
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
  }

  private mkRowsUrl(statementId: string, start: number, end: number) {
    const p = new URLSearchParams();
    p.set('statementId', statementId);
    p.set('startRow', String(start));
    p.set('endRow',   String(end));
    // NOTE: rows endpoint is GET on the base "/api/reports" per backend controller
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
      if (typeof meta.rowCount === 'number') return meta; // treat counts as "ready"
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
    // Last resort
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
