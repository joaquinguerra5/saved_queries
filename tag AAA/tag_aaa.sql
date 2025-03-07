WITH  orders AS (
  SELECT
  #dimensiones
  DATE_TRUNC(orders.registered_date, MONTH) AS month,
  case
  When dim_partner.partner_name LIKE "AMPM%" AND dim_partner.country.country_code LIKE "CR" THEN "Ampm"
  When dim_partner.partner_name LIKE "Fresh Market%" AND dim_partner.country.country_code LIKE "CR" THEN "Fresh Market"
  WHEN REGEXP_CONTAINS(lower(dim_partner.partner_name), r'la puerta del sol') AND dim_partner.country.country_code LIKE "GT" THEN "La Puerta Del Sol"
  ELSE dim_partner__franchise.franchise_name END as franchise,
  dim_partner.business_type.business_type_name as business_type,
  dim_partner.is_darkstore,
  partner_id,
  partner_name,
  orders.country.country_code AS country_code,
  dim_partner__city.name  AS city,
  franchise.franchise_id
  FROM `peya-bi-tools-pro.il_core.fact_orders` AS orders
  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dim_partner
  ON  dim_partner.partner_id = orders.restaurant.id
  LEFT JOIN  UNNEST([dim_partner.franchise]) AS dim_partner__franchise
  LEFT JOIN UNNEST([dim_partner.city]) as dim_partner__city
  WHERE  orders.registered_date between "2023-01-01" and current_date()
  AND upper(orders.business_type.business_type_name)  IN ('MARKET','KIOSKS','DRINKS','PHARMACY')
--  AND dim_partner.partner_name LIKE '%Luvebras%'
  GROUP BY 1,2,3,4,5,6,7,8,9
),


SUMMARY AS(
SELECT
orders.partner_id as partner_id,
orders.partner_name,
orders.franchise,
orders.country_code as cc,
orders.city,
CASE 
WHEN orders.franchise_id =  '0011r00002VoIJ0AAN' /*Carrefour AR*/ AND orders.country_code = 'AR' and orders.partner_name LIKE  "Carrefour - Express%" OR orders.partner_name LIKE "Carrefour Flash" THEN 'Small Supermarket'
WHEN orders.franchise_id IN (
  '0011r00002VoHgPAAV', -- DAR
  '0011r00002VoI9AAAV', -- DIA
  '0011r00002VoIK8AAN', -- AMPM
  '0011r00002VoILLAA3', -- COMODIN
  '0016900002f9CSHAA2', -- FRESH MARKET
  '0016900002w3vOKAAY', -- UNICASA
  '00169000030Wkf8AAC', -- LUVEBRAS
  '001690000350YBcAAM', -- AMARKET
  '001bO000006U6cUQAS', -- CARREFOUR DO
  '0016900002ndd0NAAQ'  -- BIGGIE
  '001bO000006UJjJQAW'  -- PLAZAS
)
THEN 'Small Supermarket'
WHEN orders.franchise IN ('Ampm','Fresh Market') THEN 'Small Supermarket' -- SUMO LOS QUE TIENEN ERROR DE FRANCHISE_ID
ELSE 'Supermarket' /*TODO EL RESTO SON SUPERMARKETS*/ END AS clasificacion,
franchise_id
FROM orders
WHERE (
(orders.franchise_id IN (
  '0011r00002VoHgPAAV',	-- Supermercado DAR
--'0011r00002VoI9AAAV',	-- Dia (EXCLUIMOS POR EL RANKER)
'0011r00002VoIJ0AAN',	-- Carrefour AR
'001bO000006U6cUQAS', -- Carrefour DO
--'0011r00002VoIK8AAN',	-- Ampm
--'0011r00002VoIK8AAN',	-- Fresh Market
'0011r00002VoILLAA3',	-- Supermercado Comodin
'0016900002f9CSHAA2',	-- Fresh Market
'0016900002w3vOKAAY',	-- Unicasa
'00169000030Wkf8AAC',	-- Luvebras
'001690000350YBcAAM',	-- Amarket
'00169000032PYVlAAO',	-- Wong
'0011r00002VoHxuAAF',	-- Tottus
'0011r00002VoI1SAAV',	-- Super Xtra
'0011r00002VoI77AAF',	-- Super 99
'0011r00002VoID8AAN',	-- Supermercado Libertad
'0011r00002VoIQMAA3',	-- Metro
'0011r00002XcCnIAAV',	-- Fidalga
'0011r00002XcDSFAA3',	-- La Anónima
'0016900002aDwwKAAS',	-- Hipermercados Olé
'0016900002aDwxrAAC',	-- Plaza Lama
'0016900002dYyCXAA0',	-- La Colonia
'0016900002dYyGsAAK',	-- Tia
'0016900002dYyUzAAK',	-- Supermercados Rey
'0016900002dYypxAAC',	-- Supermercado Gran Via
'0016900002f7gZyAAI',	-- Coral Hipermercados
'0016900002f9COxAAM',	-- Supermercados La Torre
'0016900002f9bMlAAI',	-- Santa María
'0016900002fAZOfAAO',	-- Central Madeirense
'0016900002kWsn1AAC',	-- Supermercado Los Jardines
'0016900002ncEiGAAU',	-- Super El Abastecedor
'0016900002neAJQAA2',	-- Supermercado Mixtura
'0016900002uGd9XAAS',	-- DIPROVA
'0016900002uR7uhAAC',	-- Mi Super Fresh
'0016900002w3PPrAAM',	-- Peri
'0016900002w3PYSAA2',	-- Super Compro
'00169000030SXkxAAG',	-- Real Supermercados
'00169000030SXvGAAW',	-- Tata Supermercados
'00169000030Wk7BAAS',	-- La Cadena
'00169000030ZfPNAA0',	-- Sirena
'0016900002p6pJUAAY', -- Disco
'0016900002ndd0NAAQ', -- Biggie
'0016900002fZxcpAAC', -- Vea
'0016900002dYydiAAC',  -- Jumbo
'001bO000006UJjJQAW',  -- Plazas
'001bO00000BWifaQAD'  -- 'La Puerta del Sol'
)
OR
  orders.franchise IN ('Ampm','Fresh Market')
)

)
and orders.is_darkstore is false
and orders.business_type = "Market"
GROUP BY 1,2,3,4,5,6,7
)
SELECT
SUMMARY.*,
case when LOWER(aaa_tag_same_price.TAG_SAME_PRICE) = "same price" THEN TRUE else FALSE END is_same_price
FROM SUMMARY
LEFT JOIN `peya-data-origins-pro.raw_qcommerce.gsheet_aaa_tag_same_price` AS aaa_tag_same_price
ON LOWER(franchise) = LOWER(vendor) and SUMMARY.cc = aaa_tag_same_price.country