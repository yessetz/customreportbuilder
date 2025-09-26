import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

export interface ReportMeta {
    statementId: string;
    columns?: string[];
    rowCount?: number;
    state?: string;
    pageSize?: number;
}

@Injectable({ providedIn: 'root' })
export class ReportDataService {
    private base = '/api/reports';

    constructor(private http: HttpClient) {}

    submit(sql: string) {
        return firstValueFrom(this.http.post<any>(`${this.base}/statement`, { sql }));
    }

    getMeta(statementId: string) {
        return firstValueFrom(this.http.get<ReportMeta>(`${this.base}/meta`, { params: {statementId}}));
    }

    getRows(statementId: string, startRow: number, endRow: number) {
        const params = new HttpParams()
            .set('statementId', statementId)
            .set('startRow', startRow)
            .set('endRow', endRow);
        return firstValueFrom(this.http.get<any>(this.base, { params }));
    }
}