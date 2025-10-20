import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { AgGridAngular } from 'ag-grid-angular';
import type {
  ColDef,
  GridReadyEvent,
  GridApi,
  IDatasource,
  IGetRowsParams,
} from 'ag-grid-community';
import { themeQuartz } from 'ag-grid-community';
import { compileTemplate, sqlString } from './lib/query-compiler';
import { ReportQueryService } from './services/report-query.service';
import { DimBarComponent } from './components/dim-bar/dim-bar.component';

const BASE = '/api/reports'; // no trailing slash
const STORAGE_KEY = 'crb:lastStatementId';

type Meta = {
  state?: string;
  rowCount?: number;
  pageSize?: number;
  columns?: string[];
  schema?: Array<{ name: string; type_text?: string; type_name?: string; position?: number }>;
  status?: { state?: string; message?: string };
};

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, AgGridAngular, DimBarComponent],
  template: `
    <div style="padding:12px; position: relative;">
      <h3>Databricks → Redis → API → Grid (infinite + prefetch)</h3>

      <div style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">
        <app-dim-bar></app-dim-bar>
        <button (click)="run()" [disabled]="loading">
          {{ loading ? 'Running…' : 'Run fact_product' }}
        </button>

        <button *ngIf="statementId" (click)="clearResume()" [disabled]="loading" title="Forget cached result">
          Clear resume
        </button>
      </div>

      <p *ngIf="error" style="color:#b00; white-space:pre-wrap; margin-top:8px;">
        {{ error }}
      </p>

      <div *ngIf="statementId" style="margin-top:12px;">
        <strong>statementId</strong>
        <pre>{{ statementId }}</pre>
      </div>

      <button (click)="exportCsv()" [disabled]="!statementId || loading" title="Download full result as CSV">
        Export CSV
      </button>

      <div *ngIf="meta" style="margin:12px 0;">
        <strong>meta</strong>
        <pre>{{ meta | json }}</pre>
      </div>

      <div style="border:1px solid #ddd; padding:8px;">
        <ag-grid-angular
          [theme]="theme.quartz"
          [rowModelType]="'infinite'"
          [cacheBlockSize]="cacheBlockSize"
          [maxBlocksInCache]="maxBlocksInCache"
          [maxConcurrentDatasourceRequests]="maxConcurrentDatasourceRequests"
          [columnDefs]="columnDefs"
          [defaultColDef]="defaultColDef"
          (gridReady)="onGridReady($event)"
          (sortChanged)="onSortChanged()"          
          (filterChanged)="onFilterChanged()"   
          style="color: black; width: 100%; height: 700px;">
        </ag-grid-angular>
      </div>

      <!-- Debug (last server response) -->
      <div *ngIf="debugResponse" style="margin-top:12px;">
        <strong>last rows response (debug)</strong>
        <pre>{{ debugResponse | json }}</pre>
      </div>

      <!-- Tiny toast -->
      <div
        *ngIf="toast.visible"
        style="
          position: fixed; right: 16px; bottom: 16px;
          background: rgba(0,0,0,0.85); color: #fff;
          padding: 10px 12px; border-radius: 8px; font-size: 13px;">
        {{ toast.msg }}
      </div>
    </div>
  `,
})

export class AppComponent implements OnInit {
  constructor(
    private http: HttpClient,
    private reportQuery: ReportQueryService
 ) {}

  theme = themeQuartz;
  loading = false;
  error = '';
  statementId: string | null = null;
  meta: Meta | null = null;

  // AG Grid
  private gridApi: GridApi | null = null;
  private _lastModelSig: string | null = null; // for debug

  columnDefs: ColDef[] = [];
  defaultColDef: ColDef = { resizable: true, sortable: true, filter: true, flex: 1, minWidth: 120 };

  // Infinite model settings (synced to server page size)
  cacheBlockSize = 500; // default until meta arrives
  maxBlocksInCache = 4; // tune as desired
  maxConcurrentDatasourceRequests = 2; // allow overlap (helps when user scrolls fast)

  // Prefetch cache for next block (startRow -> rows[])
  private prefetchCache = new Map<number, any[]>();
  private pendingPrefetches = new Set<number>();
  private maxPrefetchBlocks = 2; // keep at most N prefetched blocks in memory

  // Debug
  debugResponse: any = null;

  // Toast
  toast = { visible: false, msg: '', timer: null as any };

  // ---------- Lifecycle: resume on reload ----------
  async ngOnInit() {
    const last = localStorage.getItem(STORAGE_KEY);
    if (!last) return;

    try {
      this.loading = true;
      this.statementId = last;

      // Short poll for meta
      this.meta = await this.pollMeta(this.statementId, /*timeout*/ 5000, /*interval*/ 250);

      // Build columns + align block size with server
      this.setupColumnsFromMeta();
      this.cacheBlockSize = this.meta?.pageSize ?? 500;

      if (this.gridApi) {
        this.gridApi.setGridOption('cacheBlockSize', this.cacheBlockSize);
        this.gridApi.setGridOption('datasource', this.createDatasource());
      }

      this.showToast('Resumed previous result');
    } catch {
      localStorage.removeItem(STORAGE_KEY);
      this.statementId = null;
      this.meta = null;
    } finally {
      this.loading = false;
    }
  }

  // ---------- UI actions ----------
  onGridReady(e: GridReadyEvent) {
    this.gridApi = e.api;

    if (this.statementId && this.meta) {
      this.gridApi.setGridOption('cacheBlockSize', this.cacheBlockSize);
      this.gridApi.setGridOption('datasource', this.createDatasource());
    }
  }

  onSortChanged() {
    // if you keep a local prefetch cache, clear it
    this.prefetchCache?.clear?.();
    this.pendingPrefetches?.clear?.();

    // Force AG Grid Infinite Row Model to refetch rows for the new sort
    this.gridApi?.purgeInfiniteCache?.();

    // (optional) tiny feedback
    // this.showToast?.('Applying sort…', 900);
  }

  onFilterChanged() {
    // New filters → invalidate caches and refetch
    this.prefetchCache.clear();
    this.pendingPrefetches.clear();
    this.debugResponse = null;
    this.gridApi?.purgeInfiniteCache?.();

    this.showToast('Applying filter…', 900);
  }

  exportCsv() {
    if (!this.statementId) return;
    // Stream directly from the backend (no memory pressure in Angular)
    // const url = `${BASE}/export/csv?statementId=${encodeURIComponent(this.statementId)}&header=true&bom=true`;
    // window.open(url, '_blank');   // triggers a file download in a new tab
    const filterModel = JSON.stringify(this.gridApi?.getFilterModel?.() ?? {});
    const params = new URLSearchParams();
    params.set('statementId', this.statementId);
    params.set('header', 'true');
    params.set('bom', 'true');
    if (filterModel !== '{}') params.set('filterModel', filterModel);
    window.open(`${BASE}/export/csv?${params.toString()}`, '_blank');
    this.showToast?.('CSV export started', 1200);
  }

  async run() {
    this.loading = true;
    this.error = '';
    this.debugResponse = null;

    // Reset all caches
    this.prefetchCache.clear();
    this.pendingPrefetches.clear();

    // Reset grid
    if (this.gridApi) {
      const emptyDs: IDatasource = { getRows: (p: IGetRowsParams) => p.successCallback([], 0) };
      this.gridApi.setGridOption('datasource', emptyDs);
      this.gridApi.purgeInfiniteCache?.();
    }

    const querySQL = await this.reportQuery.compileQuery('fact_product');

    try {
      // 1) submit
      this.statementId = await this.reportQuery.startFactServerCompiled('fact_product');
      localStorage.setItem(STORAGE_KEY, this.statementId);

      // 2) poll meta
      this.meta = await this.pollMeta(this.statementId, /*timeout*/ 20000, /*interval*/ 300);

      // 3) columns + block size from meta
      this.setupColumnsFromMeta();
      this.cacheBlockSize = this.meta?.pageSize ?? 500;

      // 4) datasource
      if (this.gridApi) {
        this.gridApi.setGridOption('cacheBlockSize', this.cacheBlockSize);
        this.gridApi.setGridOption('datasource', this.createDatasource());
      }

      this.showToast('Query started');
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
      // ignore; still clear local
    } finally {
      localStorage.removeItem(STORAGE_KEY);
      this.statementId = null;
      this.meta = null;

      // Reset UI caches
      this.prefetchCache.clear();
      this.pendingPrefetches.clear();

      if (this.gridApi) {
        const emptyDs: IDatasource = { getRows: (p: IGetRowsParams) => p.successCallback([], 0) };
        this.gridApi.setGridOption('datasource', emptyDs);
        this.gridApi.purgeInfiniteCache?.();
      }

      this.showToast('Cleared cached result');
    }
  }

  // ---------- Infinite datasource with prefetch ----------
  private createDatasource(): IDatasource {
    const statementId = this.statementId!;
    const totalRows = this.meta?.rowCount ?? null;
    const blockSize = this.cacheBlockSize;

    return {
      getRows: async (params: IGetRowsParams) => {
        const start = params.startRow;
        const end = params.endRow;

        // NEW: capture current models from AG Grid
        const sortModelJson   = JSON.stringify(params.sortModel ?? []);
        const filterModelJson = JSON.stringify(params.filterModel ?? {});

        // Guard: if the model changed since last call, dump any stale prefetch blocks
        const currentSig = `${sortModelJson}|${filterModelJson}`;
        if (this._lastModelSig !== currentSig) {
          this.prefetchCache.clear();
          this.pendingPrefetches.clear();
          this._lastModelSig = currentSig;
        }

        const p = new URLSearchParams();
        p.set('statementId', statementId);
        p.set('startRow', String(params.startRow));
        p.set('endRow', String(params.endRow));
        if (sortModelJson !== '[]')       p.set('sortModel', sortModelJson);
        if (filterModelJson !== '{}')     p.set('filterModel', filterModelJson);

        // 1) Serve from prefetch cache if available
        const cached = this.prefetchCache.get(start);
        if (cached) {
          this.prefetchCache.delete(start);
          const lastRow = this.resolveLastRow(null, totalRows);
          params.successCallback(cached, lastRow);

          // Prefetch the next one (with the same models)
          this.maybePrefetchNext(statementId, end, blockSize, lastRow, sortModelJson, filterModelJson);
          return;
        }

        // 2) Otherwise, fetch from server (include models)
        const url = this.mkRowsUrl(statementId, start, end, sortModelJson, filterModelJson);
        try {
          const res = await this.safeGet<any>(url);
          this.debugResponse = res;

          const rows = this.mapToObjects(this.extractRows(res));
          const lastRow = this.resolveLastRow(res, totalRows);

          params.successCallback(rows, lastRow);

          // 3) Prefetch next block in background (include models)
          this.maybePrefetchNext(statementId, end, blockSize, lastRow, sortModelJson, filterModelJson);
        } catch (err) {
          console.error('getRows error', err);
          params.failCallback();
        }
      },
    };

  }

  private resolveLastRow(res: any, metaTotal: number | null): number {
    if (typeof res?.lastRow === 'number') return res.lastRow;
    if (typeof metaTotal === 'number') return metaTotal;
    return -1;
  }

  private async maybePrefetchNext(
    statementId: string,
    nextStart: number,
    blockSize: number,
    lastRow: number,
    sortModelJson?: string,
    filterModelJson?: string
  ) {
    // Don’t prefetch past the end
    if (lastRow >= 0 && nextStart >= lastRow) return;
    if (this.prefetchCache.has(nextStart) || this.pendingPrefetches.has(nextStart)) return;

    this.pendingPrefetches.add(nextStart);
    const nextEnd = nextStart + blockSize;

    // Include models so view caching matches the visible data
    const url = this.mkRowsUrl(statementId, nextStart, nextEnd, sortModelJson, filterModelJson);

    try {
      const res = await this.safeGet<any>(url);
      const rows = this.mapToObjects(this.extractRows(res));

      this.prefetchCache.set(nextStart, rows);
      while (this.prefetchCache.size > this.maxPrefetchBlocks) {
        const oldestKey = this.prefetchCache.keys().next().value;
        if (typeof oldestKey === 'number') this.prefetchCache.delete(oldestKey);
      }
    } catch {
      // ignore prefetch failures
    } finally {
      this.pendingPrefetches.delete(nextStart);
    }
  }

  // ---------- Helpers ----------
  private setupColumnsFromMeta() {
    const colNames: string[] =
      this.meta?.columns?.length
        ? this.meta.columns
        : this.meta?.schema?.length
        ? [...this.meta.schema]
            .sort((a, b) => (a.position ?? 0) - (b.position ?? 0))
            .map((c) => c.name)
        : [];

    if (colNames.length) {
      this.columnDefs = colNames.map((name: string): ColDef => ({
        headerName: name,
        field: name,
      }));
      this.gridApi?.setGridOption('columnDefs', this.columnDefs);
    }
  }

  private mapToObjects(rawRows: any[]): any[] {
    if (!rawRows?.length) return [];
    if (typeof rawRows[0] === 'object' && !Array.isArray(rawRows[0])) return rawRows;

    const colNames = this.columnDefs.map((c) => String(c.field));
    return rawRows.map((r: any[]) => {
      const o: any = {};
      for (let i = 0; i < colNames.length; i++) o[colNames[i]] = r?.[i];
      return o;
    });
  }

  private mkRowsUrl(
    statementId: string,
    start: number,
    end: number,
    sortModelJson?: string | null,
    filterModelJson?: string | null
  ) {
    const p = new URLSearchParams();
    p.set('statementId', statementId);
    p.set('startRow', String(start));
    p.set('endRow', String(end));

    // Always send both; use canonical empties to keep server behavior stable
    const sortParam   = (sortModelJson && sortModelJson !== 'null') ? sortModelJson : '[]';
    const filterParam = (filterModelJson && filterModelJson !== 'null') ? filterModelJson : '{}';

    p.set('sortModel', sortParam);
    p.set('filterModel', filterParam);

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
        meta = await firstValueFrom(
          this.http.get<Meta>(`${BASE}/meta?statementId=${statementId}`)
        );
      } catch {
        if (Date.now() - start > timeoutMs) throw new Error('Timed out waiting for statement meta.');
        await this.delay(intervalMs);
        continue;
      }
      const state = (meta.state ?? '').toUpperCase();
      if (typeof meta.rowCount === 'number') return meta;
      if (['SUCCEEDED', 'DONE', 'COMPLETED'].includes(state)) return meta;
      if (Date.now() - start > timeoutMs) throw new Error('Timed out waiting for statement meta.');
      await this.delay(intervalMs);
    }
  }

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

  private delay(ms: number) {
    return new Promise((res) => setTimeout(res, ms));
  }

  private showToast(msg: string, ms = 3000) {
    this.toast.msg = msg;
    this.toast.visible = true;
    if (this.toast.timer) clearTimeout(this.toast.timer);
    this.toast.timer = setTimeout(() => {
      this.toast.visible = false;
      this.toast.timer = null;
    }, ms);
  }

  private normalizeHttpError(err: unknown): string {
    if (err instanceof HttpErrorResponse) {
      const body = typeof err.error === 'string' ? err.error : JSON.stringify(err.error);
      return `${err.status} ${err.statusText} — ${err.url}\n${body}`;
    }
    if (err instanceof Error) return err.message;
    try {
      return JSON.stringify(err);
    } catch {
      return String(err);
    }
  }

  private async loadQuerySql(name: string): Promise<string> {
    return await firstValueFrom(
      this.http.get(`/assets/queries/${name}.sql`, { responseType: 'text' })
    );
  }
}
