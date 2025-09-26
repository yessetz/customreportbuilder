import { Component } from '@angular/core';
import { ReportGridComponent } from './report-grid.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [ReportGridComponent],
  template: `<app-report-grid></app-report-grid>`,
})
export class AppComponent {}