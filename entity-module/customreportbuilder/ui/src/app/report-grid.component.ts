import { Component, ViewChild } from '@angular/core';
import { NgIf } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AgGridAngular, AgGridModule } from 'ag-grid-angular';
import { ColDef, GridOptions, IGetRowsParams } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-quartz.css';
import { ReportDataService } from './report-data.service';

@Component({
    selector: 'app-report-grid',
    standalone: true,
    imports: [NgIf, FormsModule, AgGridModule],
    template: `
    <div class="toolbar">
        <textarea [(ngModel)]="sql" rows="3"></textarea>
        <button (click)="run()" [disabled]="loading">Run</button>
        <span *ngIf="meta?.state">State: {{meta?.state}}</span>
        <span *ngIf="meta?.rowCount != null"> | Rows: {{meta?.rowCount}}</span>
    </div>
    <div class="ag-theme-quartz" style="height: 600px; width: 100%;">
        <ag-grid-angular [gridOptions]="gridOptions"></ag-grid-angular>
    </div>
    `,
    styles: [`
        .toolbar {display: flex; gap: 10px; margin-bottom: 10px;} textarea {flex: 1}
    `]
})
export class ReportGridComponent {
    @ViewChild(AgGridAngular) grid!: AgGridAngular;

    sql = 'SELECT 1';
    meta: any;
    statementId: string = '';
    columns: string[] = [];
    loading = false;

    columnDefs: ColDef[] = [];
    gridOptions: GridOptions = {
        rowModelType: 'infinite',
        cacheBlockSize: 100,
        pagination: false,
        datasource: {
            getRows: async (params: IGetRowsParams) => {
                if (!this.statementId) {
                    params.successCallback([], 0);
                    return;
                }
                try {
                    const resp = await this.svc.getRows(this.statementId, params.startRow, params.endRow);
                    const rows = (resp.rows as any[]).map((r: any[]) => {
                        const o: any = {};
                        this.columns.forEach((c, i) => o[c] = r[i]);
                        return o;
                    });
                    params.successCallback(rows, resp.lastRow ?? undefined);
                } catch {
                    params.failCallback();
                }
            }
        },
        defaultColDef: { resizable: true, sortable: false, filter: false }
    };

    cconstructor(private ds: ReportDataService) {}

    async onGridReady() {
    this.rowData = await this.ds.runSelectOne();
    }


    async run() {
        this.loading = true;
        const submit = await this.svc.submit(this.sql);
        this.statementId = submit.statementId;
        this.meta = submit;
        this.columns = submit.columns || [];

        this.setCols();
        this.pollMeta();
        this.grid.api.setGridOption('datasource', this.gridOptions.datasource!);
        this.grid.api.refreshInfiniteCache();
    }

    private setCols() {
        this.columnDefs = this.columns.map(c => ({ field: c, minWidth: 120 }));
        if (this.grid?.api) {
            this.grid.api.setGridOption('columnDefs', this.columnDefs);
        }
    }

    private pollMeta() {
        const id = setInterval(async () => {
            const m = await this.svc.getMeta(this.statementId);
            this.meta = m;
            if (m.columns && this.columns.length === 0) {
                this.columns = m.columns;
                this.setCols();
            }
            if (['FAILED', 'CANCELED', 'SUCCEEDED', 'FINISHED'].includes((m.state || '').toUpperCase())) {
                clearInterval(id);
                this.loading = false;
                this.grid.api.refreshInfiniteCache();
            }
        }, 1200);
    }
}