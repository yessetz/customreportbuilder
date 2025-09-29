import { Component } from '@angular/core';
import { AgGridAngular } from 'ag-grid-angular';
import type { ColDef, GridApi, GridReadyEvent } from 'ag-grid-community';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [AgGridAngular],
  template: `
    <div style="padding:12px;">
      <button (click)="loadTest()">Load SELECT 1</button>
      <div class="ag-theme-quartz" style="height:400px;width:100%;margin-top:12px;">
        <ag-grid-angular
          [columnDefs]="columnDefs"
          [rowData]="rowData"
          (gridReady)="onGridReady($event)">
        </ag-grid-angular>
      </div>
    </div>
  `,
})


export class AppComponent {
  columnDefs: ColDef[] = [{ headerName: 'demo', field: 'demo' }];
  rowData: any[] = [];
  private api?: GridApi;

  onGridReady(e: GridReadyEvent) {
    this.api = e.api;
    this.rowData = [{ demo: 1 }];           // visible immediately
  }

  loadTest() {
    if (!this.api) return;
    this.rowData = [{ demo: Math.floor(Math.random() * 1000) }]; // visible change
  }
}
