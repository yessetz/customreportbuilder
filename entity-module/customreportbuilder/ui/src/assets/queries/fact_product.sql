SELECT
  p.sku        AS 'sku name',
  p.name       AS 'product name',
  c.category_name AS 'category name',
  p.price      AS 'price',
  p.created_at AS 'created date',
  p.modified_at AS 'modified date'
FROM analytics.mm.fact_product p
LEFT JOIN analytics.mm.dim_category c ON p.category_id = c.category_id
WHERE 1=1
    AND YEAR(p.created_at) BETWEEN 2023 AND 2025
/*WHERE:date_range*/     -- e.g., AND p.created_at BETWEEN :start AND :end
/*WHERE:category_id*/    -- e.g., AND c.category_id = :categoryId
/*GROUP_BY*/
/*HAVING*/
/*ORDER_BY*/
/*LIMIT*/