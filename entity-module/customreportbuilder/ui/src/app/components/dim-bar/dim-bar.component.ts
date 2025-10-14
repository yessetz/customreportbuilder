import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { ReportQueryService } from '../../services/report-query.service';

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
      <div style="display:flex; gap:6px; align-items:end;">
        <label style="display:flex; flex-direction:column; font-size:12px;">
          Brand (multi)
          <select multiple [size]="Math.min(8, brands.length || 8)"
                  (change)="onBrandChange($event)">
            <option *ngFor="let b of brands" [value]="b.id">{{ b.name }}</option>
          </select>
        </label>
      </div>

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

  constructor(
    private http: HttpClient,
    private reportQuery: ReportQueryService
  ) {}

  async ngOnInit() {
    try {
      this.categories = await firstValueFrom(
        this.http.get<Category[]>('/assets/dims/categories.json')
      );
      this.brands = await firstValueFrom(
        this.http.get<Brand[]>('/assets/dims/brands.json')
      );
    } catch {
      this.categories = [];
      this.brands = [];
    }
  }

  apply() {
    this.reportQuery.setFilters({
      dateFrom: this.dateFrom || null,
      dateTo: this.dateTo || null,
      categoryId: this.categoryId || null,
      brandIds: this.brandIds.length ? this.brandIds : null,   // <-- NEW
    });
    // No auto-run: user will click "Run fact_product"
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
    const selected = Array.from(el.selectedOptions).map(o => o.value);
    this.brandIds = selected;
    this.apply();
  }

}
