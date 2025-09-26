import { bootstrapApplication } from '@angular/platform-browser';
import { ReportGridComponent } from './app/report-grid.component';
import { provideHttpClient } from '@angular/common/http';

bootstrapApplication(ReportGridComponent, {
    providers: [provideHttpClient()]
}).catch(err => console.error(err));