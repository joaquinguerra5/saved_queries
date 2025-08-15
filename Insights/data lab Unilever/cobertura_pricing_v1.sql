SELECT
  dvps.snapshot_date,
  dvps.master_code,
  pim.piece_barcode,
  dvps.master_category_names.level_one,
  dvps.master_category_names.level_two,
  dvps.master_category_names.level_three,
  dvps.product_name,
  pim.brands_name,
  pim.brand_owner_name,
  dvps.remote_vendor_id as partner_id,
  CASE
    WHEN dhp.is_darkstore THEN 'Dmart'
    WHEN aaa.partner_id IS NOT NULL THEN 'AAA'
    ELSE dhp.business_category.name
  END as business_type,
  dhp.country_id,
  dhp.country_name,
  dhp.city_id,
  dhp.city_name,
  dhp.zone_id,
  zone_name,
  dvps.catalog_price_lc,
  dvps.catalog_original_price_lc,
  AVG(CASE WHEN dvps.product_is_active THEN dvps.catalog_price_lc ELSE NULL END) OVER(PARTITION BY dvps.master_code,dvps.snapshot_date) avg_active_price_master_code,
  dvps.product_is_active
FROM
  `peya-bi-tools-pro.il_qcommerce.dim_vendor_product_snapshot` dvps
LEFT JOIN
  `peya-bi-tools-pro.il_core.dim_historical_partners` dhp
ON
  dhp.restaurant_id = dvps.remote_vendor_id AND dvps.snapshot_date = dhp.yyyymmdd
LEFT JOIN
  `peya-data-origins-pro.cl_dmarts.pim_product` pim
ON
  pim.product_id = dvps.master_code
LEFT JOIN
  `peya-food-and-groceries.automated_tables_reports.partners_aaa_snapshot` aaa
ON
  aaa.partner_id = dvps.remote_vendor_id AND aaa.snapshot_date = dvps.snapshot_date
WHERE
  dvps.snapshot_date >= '2025-07-15'
AND
  dhp.is_active
AND
  dhp.is_online
AND
  dvps.status = 'ACTIVE'
AND
  pim.brand_owner_name = 'Unilever'
AND
  dhp.country_name = 'Argentina'
-- AND
--   dvps.master_code = 'latamTQ79Z7'
AND
  dvps.barcodes LIKE '%7794000006072%'
