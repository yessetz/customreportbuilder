SELECT
  CAST(b.brand_id AS STRING) AS id,
  b.brand_name               AS name
FROM analytics.mm.dim_brand AS b
WHERE b.brand_id IS NOT NULL AND b.brand_name IS NOT NULL
GROUP BY b.brand_id, b.brand_name
ORDER BY name;
