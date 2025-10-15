import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { ReportQueryService } from '../../services/report-query.service';
import { DimsDataService } from '../../services/dims-data.service';

type Category = { id: string; name: string };
type Brand = { id: string; name: string };

@Component({
  selector: 'app-dim-bar',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div style="display:flex; gap:12px; align-items:end; flex-wrap: wrap; margin: 8px 0;">
      <!-- Date range -->
      <div style="display:flex; gap:6px; align-items:end;">
        <label style="display:flex; flex-direction:column; font-size:12px;">
          From
          <input type="date" [(ngModel)]="dateFrom" (change)="apply()" />
        </label>
        <label style="display:flex; flex-direction:column; font-size:12px;">
          To
          <input type="date" [(ngModel)]="dateTo" (change)="apply()" />
        </label>
      </div>

      <!-- Category -->
      <div style="display:flex; gap:6px; align-items:end;">
        <label style="display:flex; flex-direction:column; font-size:12px;">
          Category
          <select [(ngModel)]="categoryId" (change)="apply()">
            <option [ngValue]="null">— Any —</option>
            <option *ngFor="let c of categories" [ngValue]="c.id">{{ c.name }}</option>
          </select>
        </label>
      </div>

      <!-- Brand (multi-select) -->
      <label style="display:flex; flex-direction:column; font-size:12px;">
        Brand (multi)
        <select multiple [size]="brandListSize" (change)="onBrandChange($event)">
          <option *ngFor="let b of brands" [value]="b.id" [selected]="brandIds.includes(b.id)">
            {{ b.name }}
          </option>
        </select>
      </label>

      <!-- Reset -->
      <button (click)="reset()" style="height:32px;">Reset filters</button>
    </div>
  `,
})
export class DimBarComponent implements OnInit {
  categories: Category[] = [];
  brands: Brand[] = [];
  brandIds: string[] = [];

  // Local state (not stored in app.component)
  dateFrom: string | null = null; // ISO yyyy-mm-dd
  dateTo:   string | null = null;
  categoryId: string | null = null;

  // computed size for the multi-select
  get brandListSize(): number {
    return Math.min(8, this.brands.length || 8);
  }

  constructor(
    private http: HttpClient,
    private reportQuery: ReportQueryService,
    private dims: DimsDataService
  ) {}


  async ngOnInit() {
    try {
      this.categories = await this.dims.loadCategories();
    } catch { this.categories = []; }
    try {
      this.brands = await this.dims.loadBrands();
    } catch { this.brands = []; }
  }

  apply() {
    this.reportQuery.setFilters({
      dateFrom: this.dateFrom || null,
      dateTo: this.dateTo || null,
      categoryId: this.categoryId || null,
      brandIds: this.brandIds.length ? this.brandIds : null,
    });
  }

  reset() {
    this.dateFrom = null;
    this.dateTo = null;
    this.categoryId = null;
    this.brandIds = [];
    this.reportQuery.resetFilters();
  }

  onBrandChange(evt: Event) {
    const el = evt.target as HTMLSelectElement;
    this.brandIds = Array.from(el.selectedOptions).map(o => o.value);
    this.apply();
  }
}
