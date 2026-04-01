WITH 
brand_cpg_exception_list AS (
  SELECT
    *
  FROM
    `peya-data-origins-pro.cl_qcommerce.cpg_brand_mapping_exceptional_list` el
  QUALIFY ROW_NUMBER() OVER latest_update = 1
    WINDOW latest_update AS (
    PARTITION BY global_entity_id, product_cpg, product_brand
    ORDER BY COALESCE(
    SAFE.PARSE_DATE('%d/%m/%Y', is_updated_by_central),
    SAFE.PARSE_DATE('%m/%d/%Y', is_updated_by_central)
) DESC NULLS LAST)
  ORDER BY
    1,2,3
),

products AS
(SELECT
  remote_vendor_id as partner_id,
  CASE
    WHEN is_dmart THEN 'Dmart'
    WHEN is_aaa THEN 'AAA'
    ELSE 'Local Shops'
  END as tipo_tienda,
  dhp.business_category.name as business_category_name,
  dhp.business_category.name_salesforce as salesforce_name,
  dvps.master_code,
  pim.product_name,
  dhp.province_name,
  dhp.zone_name,
  dhp.area_name,
  CASE
    WHEN INSTR(dvps.barcodes,',') > 0 AND STARTS_WITH(dvps.barcodes, "0") = False THEN SUBSTR(dvps.barcodes,1,INSTR(dvps.barcodes,',')-1)
    WHEN INSTR(dvps.barcodes,',') > 0 AND STARTS_WITH(dvps.barcodes, "0") THEN SUBSTR(dvps.barcodes,2,INSTR(dvps.barcodes,',')-2)
    ELSE dvps.barcodes
  END AS barcode,
  COALESCE(sel.correct_product_cpg, el.correct_product_cpg, pim.brand_owner_name, 'Others') AS product_cpg,
  COALESCE(INITCAP(dvps.brand_name), INITCAP(pim.brands_name), 'Others') AS product_brand,
  dvps.product_is_active,
  dvps.catalog_price_lc,
  COALESCE(stock_end_of_day,0) > COALESCE(im.sales_buffer,0) + COALESCE(stock_reserved_quantity_day,0) as stock
FROM
  `peya-bi-tools-pro.il_qcommerce.dim_vendor_product_snapshot` dvps
LEFT JOIN
  `peya-datamarts-pro.dm_dmarts.dmarts_inventory` im
ON
  im.warehouse_id = dvps.warehouse_id AND dvps.sku = im.sku_id AND dvps.snapshot_date = im.date_local
LEFT JOIN
  `peya-data-origins-pro.cl_dmarts.pim_product` pim
ON
  pim.product_id = dvps.master_code
LEFT JOIN brand_cpg_exception_list  AS el
ON dvps.global_entity_id = el.global_entity_id
AND COALESCE(pim.brand_owner_name, 'Others') = el.product_cpg
AND COALESCE(INITCAP(dvps.brand_name), INITCAP(pim.brands_name), 'Others') = INITCAP(el.product_brand)
LEFT JOIN `peya-data-origins-pro.cl_qcommerce.cpg_barcode_mapping_exceptional_list` AS sel
ON dvps.global_entity_id = sel.global_entity_id
AND COALESCE(el.correct_product_cpg, pim.brand_owner_name, 'Others') = sel.product_cpg
AND COALESCE(INITCAP(dvps.brand_name), INITCAP(pim.brands_name), 'Others') = sel.product_brand
AND (CASE
  WHEN INSTR(dvps.barcodes,',') > 0 AND STARTS_WITH(dvps.barcodes, "0") = False THEN SUBSTR(dvps.barcodes,1,INSTR(dvps.barcodes,',')-1)
  WHEN INSTR(dvps.barcodes,',') > 0 AND STARTS_WITH(dvps.barcodes, "0") THEN SUBSTR(dvps.barcodes,2,INSTR(dvps.barcodes,',')-2)
  ELSE dvps.barcodes
END) = sel.barcode
LEFT JOIN
  `peya-bi-tools-pro.il_core.dim_historical_partners` dhp
ON
  dhp.restaurant_id = dvps.remote_vendor_id AND dhp.yyyymmdd = dvps.snapshot_date
WHERE
  snapshot_date BETWEEN '2026-01-04' AND '2026-01-04'
AND
  UPPER(dvps.country_code) = 'AR'
AND
  dhp.is_active
AND
  dhp.is_online
-- AND
--   dhp.is_darkstore
-- AND
--   dhp.business_category.name = 'Supermercados'
-- AND
--   remote_vendor_id = 286798
AND
  status = 'ACTIVE'
-- AND
--   dvps.brand_name LIKE 'Coca%';
 AND
   dhp.business_category.name_salesforce NOT IN ('pets','hardware','frozen_food','home_and_gifts','flowers_and_plants','fishery','electronics','pets','pasta_shop','nuts_and_dried_fruits','stationery_and_books')

) 

SELECT
  --salesforce_name,
  province_name,
  area_name,
  COUNT(DISTINCT CASE WHEN product_cpg = 'Unilever' THEN partner_id END) as partners_w_unilever,
  COUNT(DISTINCT CASE WHEN product_is_active AND product_cpg = 'Unilever' THEN partner_id END) as partners_w_unilever_active,
  COUNT(DISTINCT CASE WHEN NOT product_is_active AND product_cpg = 'Unilever' THEN partner_id END) as partners_w_unilever_inactive,
  COUNT(DISTINCT partner_id) as partners,
  COUNT(DISTINCT CASE WHEN product_cpg = 'Unilever' THEN CONCAT(partner_id,master_code) END) as vendor_products_unilever
FROM
  products
-- WHERE
--   product_cpg = 'Unilever'
GROUP BY
1,2