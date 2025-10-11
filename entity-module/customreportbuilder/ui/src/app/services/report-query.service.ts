import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { compileTemplate, sqlString } from '../lib/query-compiler';

export interface ProductFilters {
  dateFrom?: string | null;
  dateTo?: string | null;
  categoryId?: string | null;
}

@Injectable({ providedIn: 'root' })
export class ReportQueryService {
  private filters: ProductFilters = {};

  constructor(private http: HttpClient) {}

  /** Update any subset of filters (call from your dim bars later). */
  setFilters(patch: ProductFilters) {
    this.filters = { ...this.filters, ...patch };
  }

  /** Optional: reset everything */
  resetFilters() {
    this.filters = {};
  }

  /** Loads the base SQL and compiles it with current filters. */
  async compileQuery(name: string): Promise<string> {
    const baseSql = await firstValueFrom(
      this.http.get(`/assets/queries/${name}.sql`, { responseType: 'text' })
    );

    const wheres: { key: string; sql: string }[] = [];
    const f = this.filters;

    if (f.dateFrom && f.dateTo) {
      wheres.push({
        key: 'date_range',
        sql: `p.created_at >= DATE ${sqlString(f.dateFrom)} AND p.created_at < DATE ${sqlString(this.nextDay(f.dateTo))}`
      });
    }
    if (f.categoryId) {
      wheres.push({
        key: 'category_id',
        sql: `c.category_id = ${sqlString(f.categoryId)}`
      });
    }

    const compiled = compileTemplate(baseSql, {
      joins: [],     // add future joins here, e.g., brand, vendor, etc.
      wheres,
      // orderBy: ['p.created_at DESC'],
      // limit: 1000,
    });

    return compiled;
  }

  private nextDay(iso: string): string {
    const d = new Date(iso + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    return d.toISOString().slice(0, 10);
  }
}
