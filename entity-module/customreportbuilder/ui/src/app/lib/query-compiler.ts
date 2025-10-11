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

  // Helper to replace a marker or (for WHERE) append right after the first WHERE
  const injectByMarker = (markerPrefix: string, key: string, text: string) => {
    const marker = `/*${markerPrefix}:${key}*/`;
    if (sql.includes(marker)) {
      sql = sql.replace(marker, text ? `${text}\n` : '');
      return true;
    }
    return false;
  };

  // JOINS
  for (const j of parts.joins ?? []) {
    if (!injectByMarker('JOIN', j.key, j.sql)) {
      // Fallback: insert before WHERE if no explicit marker for that key
      const whereIdx = sql.toUpperCase().indexOf('\nWHERE ');
      if (whereIdx > -1) {
        sql = `${sql.slice(0, whereIdx)}\n${j.sql}\n${sql.slice(whereIdx)}`;
      } else {
        sql = `${sql.trimEnd()}\n${j.sql}\n`;
      }
    }
  }

  // WHEREs
  const groupedWheres: Record<string, string[]> = {};
  for (const w of parts.wheres ?? []) {
    groupedWheres[w.key] = groupedWheres[w.key] || [];
    groupedWheres[w.key].push(w.sql);
  }
  for (const [key, arr] of Object.entries(groupedWheres)) {
    const combined = arr.filter(Boolean).map(s => `  AND ${s}`).join('\n');
    if (!injectByMarker('WHERE', key, combined)) {
      // Fallback: append to first WHERE
      const m = /\bWHERE\b/i.exec(sql);
      if (m) {
        const idx = m.index + m[0].length;
        sql = `${sql.slice(0, idx)}\n${combined}\n${sql.slice(idx)}`;
      } else {
        sql = `${sql.trimEnd()}\nWHERE 1=1\n${combined}\n`;
      }
    }
  }

  // GROUP BY
  if (parts.groupBy?.length) {
    const block = `GROUP BY\n  ${parts.groupBy.join(', ')}`;
    sql = sql.replace('/*GROUP_BY*/', `${block}\n`);
  } else {
    sql = sql.replace('/*GROUP_BY*/', '');
  }

  // HAVING
  if (parts.having?.length) {
    const block = `HAVING\n  ${parts.having.join('\n  AND ')}`;
    sql = sql.replace('/*HAVING*/', `${block}\n`);
  } else {
    sql = sql.replace('/*HAVING*/', '');
  }

  // ORDER BY
  if (parts.orderBy?.length) {
    const block = `ORDER BY\n  ${parts.orderBy.join(', ')}`;
    sql = sql.replace('/*ORDER_BY*/', `${block}\n`);
  } else {
    sql = sql.replace('/*ORDER_BY*/', '');
  }

  // LIMIT
  if (typeof parts.limit === 'number' && isFinite(parts.limit)) {
    sql = sql.replace('/*LIMIT*/', `LIMIT ${parts.limit}\n`);
  } else {
    sql = sql.replace('/*LIMIT*/', '');
  }

  return sql.trim() + '\n';
}
