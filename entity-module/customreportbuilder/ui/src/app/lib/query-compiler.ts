// Minimal client-side SQL compiler for template markers
// Supports markers: /*JOIN:<key>*/, /*WHERE:<key>*/, /*GROUP_BY*/, /*HAVING*/, /*ORDER_BY*/, /*LIMIT*/

export type JoinClause = { key: string; sql: string };      // e.g., { key: '__extra', sql: 'LEFT JOIN ...' }
export type WhereClause = { key: string; sql: string };     // e.g., { key: 'date_range', sql: "p.created_at >= DATE '2023-01-01' AND p.created_at < DATE '2026-01-01'" }

export interface QueryParts {
  joins?: JoinClause[];
  wheres?: WhereClause[];
  groupBy?: string[];  // e.g., ['p.product_id', 'c.category_name']
  having?: string[];   // e.g., ['COUNT(*) > 1']
  orderBy?: string[];  // e.g., ['p.created_at DESC']
  limit?: number | null;
}

// Basic escaping for string literals
export function sqlString(v: string): string {
  return `'${String(v).replace(/'/g, "''")}'`;
}

export function compileTemplate(baseSql: string, parts: QueryParts): string {
  let sql = baseSql;

  // Replace a marker (/*JOIN:key*/ or /*WHERE:key*/) if present
  const injectByMarker = (markerPrefix: 'JOIN' | 'WHERE', key: string, text: string) => {
    const marker = `/*${markerPrefix}:${key}*/`;
    if (sql.includes(marker)) {
      sql = sql.replace(marker, text ? `${text}\n` : '');
      return true;
    }
    return false;
  };

  // ---------- JOINS ----------
  for (const j of parts.joins ?? []) {
    if (!j?.sql) continue;
    if (!injectByMarker('JOIN', j.key, j.sql)) {
      // Fallback: insert join before the first WHERE (any casing), or at the end if none
      const m = /\bWHERE\b/i.exec(sql);
      if (m) {
        sql = `${sql.slice(0, m.index)}\n${j.sql}\n${sql.slice(m.index)}`;
      } else {
        sql = `${sql.trimEnd()}\n${j.sql}\n`;
      }
    }
  }

  // ---------- WHEREs ----------
  const groupedWheres: Record<string, string[]> = {};
  for (const w of parts.wheres ?? []) {
    if (!w?.sql) continue;
    (groupedWheres[w.key] ??= []).push(w.sql);
  }

  for (const [key, arr] of Object.entries(groupedWheres)) {
    // Combine same-key clauses under one marker
    const combined = arr.map(s => `  AND ${s}`).join('\n');

    // 1) Best: replace the matching marker spot
    if (injectByMarker('WHERE', key, combined)) continue;

    // 2) If template has "WHERE 1=1", inject right after it
    if (/\bWHERE\b\s*1\s*=\s*1/i.test(sql)) {
      sql = sql.replace(/\bWHERE\b\s*1\s*=\s*1/i, (m) => `${m}\n${combined}`);
      continue;
    }

    // 3) If template has some WHERE (without 1=1), inject right after the WHERE token
    const whereToken = /\bWHERE\b/i.exec(sql);
    if (whereToken) {
      const idx = whereToken.index + whereToken[0].length;
      sql = `${sql.slice(0, idx)}\n${combined}\n${sql.slice(idx)}`;
      continue;
    }

    // 4) No WHERE in template → create one with our clauses
    sql = `${sql.trimEnd()}\nWHERE 1=1\n${combined}\n`;
  }

  // Normalize the edge-case: "WHERE\n  AND ..." → "WHERE 1=1\n  AND ..."
  sql = sql.replace(/\bWHERE\s*\n\s*AND\b/gi, 'WHERE 1=1\n  AND');

  // ---------- GROUP BY / HAVING / ORDER BY / LIMIT ----------
  if (parts.groupBy?.length) {
    const block = `GROUP BY\n  ${parts.groupBy.join(', ')}`;
    sql = sql.replace('/*GROUP_BY*/', `${block}\n`);
  } else {
    sql = sql.replace('/*GROUP_BY*/', '');
  }

  if (parts.having?.length) {
    const block = `HAVING\n  ${parts.having.join('\n  AND ')}`;
    sql = sql.replace('/*HAVING*/', `${block}\n`);
  } else {
    sql = sql.replace('/*HAVING*/', '');
  }

  if (parts.orderBy?.length) {
    const block = `ORDER BY\n  ${parts.orderBy.join(', ')}`;
    sql = sql.replace('/*ORDER_BY*/', `${block}\n`);
  } else {
    sql = sql.replace('/*ORDER_BY*/', '');
  }

  if (typeof parts.limit === 'number' && isFinite(parts.limit)) {
    sql = sql.replace('/*LIMIT*/', `LIMIT ${parts.limit}\n`);
  } else {
    sql = sql.replace('/*LIMIT*/', '');
  }

  return sql.trim() + '\n';
}
