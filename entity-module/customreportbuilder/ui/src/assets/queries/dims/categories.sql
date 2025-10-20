-- categories.sql
SELECT
  CAST(c.category_id AS STRING) AS id,
  c.category_name               AS name
FROM analytics.mm.dim_category AS c
WHERE c.category_id IS NOT NULL AND c.category_name IS NOT NULL
GROUP BY c.category_id, c.category_name
ORDER BY name;
