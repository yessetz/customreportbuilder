-- Base template: fact_product
-- Required base join kept because we select c.category_name
SELECT
  p.sku          AS `sku name`,
  p.name         AS `product name`,
  c.category_name AS `category name`,
  p.price        AS `price`,
  p.created_at   AS `created date`,
  p.modified_at  AS `modified date`
FROM analytics.mm.fact_product AS p
LEFT JOIN analytics.mm.dim_category AS c
  ON p.category_id = c.category_id

/*JOIN:__extra*/

WHERE 1=1
/*WHERE:date_range*/
/*WHERE:category_id*/

/*GROUP_BY*/
/*HAVING*/
/*ORDER_BY*/
/*LIMIT*/
