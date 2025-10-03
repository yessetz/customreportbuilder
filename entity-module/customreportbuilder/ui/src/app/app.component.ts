import { Component, inject, OnInit } from '@angular/core';
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
  imports: [AgGridAngular],
  template: `
    <div style="padding:12px;">
      <h3>Databricks → Redis → API → Grid (infinite model)</h3>

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
          [rowModelType]="'infinite'"
          [cacheBlockSize]="cacheBlockSize"
          [maxBlocksInCache]="maxBlocksInCache"
          [columnDefs]="columnDefs"
          [defaultColDef]="defaultColDef"
          (gridReady)="onGridReady($event)"
          style="color: black; width: 100%; height: 700px;">
        </ag-grid-angular>
      </div>

      <div *ngIf="debugResponse" style="margin-top:12px;">
        <strong>last rows response (debug)</strong>
        <pre>{{ debugResponse | json }}</pre>
      </div>
    </div>
  `,
})
export class AppComponent implements OnInit {
  private http = inject(HttpClient);

  theme = themeQuartz;
  loading = false;
  error = '';
  statementId: string | null = null;
  meta: Meta | null = null;

  // AG Grid
  private gridApi: GridApi | null = null;

  columnDefs: ColDef[] = [];
  defaultColDef: ColDef = { resizable: true, sortable: true, filter: true, flex: 1, minWidth: 120 };

  // Infinite model settings (synced to server page size)
  cacheBlockSize = 500; // default until meta arrives
  maxBlocksInCache = 4; // tune as desired

  // Debug
  debugResponse: any = null;
  resumeNotice = '';

  // ---------- Lifecycle: resume on reload ----------
  async ngOnInit() {
    const last = localStorage.getItem(STORAGE_KEY);
    if (!last) return;

    try {
      this.loading = true;
      this.resumeNotice = 'Resuming last result…';
      this.statementId = last;

      // Short poll for meta
      this.meta = await this.pollMeta(this.statementId, /*timeout*/ 5000, /*interval*/ 250);

      // Build columns + align block size with server
      this.setupColumnsFromMeta();
      this.cacheBlockSize = this.meta?.pageSize ?? 500;

      // If grid is already ready, attach datasource now
      if (this.gridApi) {
        this.gridApi.setGridOption('cacheBlockSize', this.cacheBlockSize);
        this.gridApi.setGridOption('datasource', this.createDatasource());
      }

      this.resumeNotice = 'Resumed from previous result.';
    } catch {
      localStorage.removeItem(STORAGE_KEY);
      this.statementId = null;
      this.meta = null;
      this.resumeNotice = '';
    } finally {
      this.loading = false;
    }
  }

  // ---------- UI actions ----------
  onGridReady(e: GridReadyEvent) {
    this.gridApi = e.api;

    if (this.statementId && this.meta) {
      // If resume logic already set everything up
      this.gridApi.setGridOption('cacheBlockSize', this.cacheBlockSize);
      this.gridApi.setGridOption('datasource', this.createDatasource());
    }
  }

  async run() {
    this.loading = true;
    this.error = '';
    this.debugResponse = null;

    // Reset grid cache / datasource to empty until new one is set
    if (this.gridApi) {
      const emptyDs: IDatasource = {
        getRows: (p: IGetRowsParams) => p.successCallback([], 0),
      };
      this.gridApi.setGridOption('datasource', emptyDs);
      this.gridApi.purgeInfiniteCache?.();
    }

    const querySQL = 'SELECT 1 AS demo';

    try {
      // 1) submit
      const submit = await firstValueFrom(
        this.http.post<{ statementId: string }>(`${BASE}/statement`, { sql: querySQL })
      );
      this.statementId = submit.statementId;
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

      // Reset grid view to empty datasource
      if (this.gridApi) {
        const emptyDs: IDatasource = { getRows: (p: IGetRowsParams) => p.successCallback([], 0) };
        this.gridApi.setGridOption('datasource', emptyDs);
        this.gridApi.purgeInfiniteCache?.();
      }

      this.resumeNotice = '';
    }
  }

  // ---------- Infinite datasource ----------
  private createDatasource(): IDatasource {
    const statementId = this.statementId!;
    const totalRows = this.meta?.rowCount ?? null;

    return {
      getRows: async (params: IGetRowsParams) => {
        const start = params.startRow;
        const end = params.endRow;
        const url = this.mkRowsUrl(statementId, start, end);

        try {
          const res = await this.safeGet<any>(url);
          this.debugResponse = res;

          const rows = this.mapToObjects(this.extractRows(res));

          const lastRow =
            typeof res?.lastRow === 'number'
              ? res.lastRow
              : typeof totalRows === 'number'
              ? totalRows
              : -1; // unknown

          params.successCallback(rows, lastRow);
        } catch (err) {
          console.error('getRows error', err);
          params.failCallback();
        }
      },
    };
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
    // Already objects?
    if (typeof rawRows[0] === 'object' && !Array.isArray(rawRows[0])) {
      return rawRows;
    }
    // Convert arrays → objects using current columnDefs
    const colNames = this.columnDefs.map((c) => String(c.field));
    return rawRows.map((r: any[]) => {
      const o: any = {};
      for (let i = 0; i < colNames.length; i++) {
        o[colNames[i]] = r?.[i];
      }
      return o;
    });
  }

  private mkRowsUrl(statementId: string, start: number, end: number) {
    const p = new URLSearchParams();
    p.set('statementId', statementId);
    p.set('startRow', String(start));
    p.set('endRow', String(end));
    // rows endpoint is GET on "/api/reports"
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
          this.http.get<Meta>(`${BASE}/statement?statementId=${statementId}`)
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
}
