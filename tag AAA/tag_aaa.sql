WITH  orders AS (
  SELECT
  DISTINCT
  dp.franchise.franchise_name as franchise,
  dp.business_type.business_type_name as business_type,
  dp.is_darkstore,
  partner_id,
  partner_name,
  orders.country.country_code AS country_code,
  dp.city.name  AS city,
  franchise.franchise_id
  FROM `peya-bi-tools-pro.il_core.fact_orders` AS orders
  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp
  ON  dp.partner_id = orders.restaurant.id
  WHERE  orders.registered_date between "2023-01-01" and current_date()
  AND upper(dp.business_type.business_type_name)  = 'MARKET'
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
  '0016900002f9CSHAA2', -- FRESH MARKET
  '0016900002w3vOKAAY', -- UNICASA
  '001690000350YBcAAM', -- AMARKET
  '001bO000006U6cUQAS', -- CARREFOUR DO
  '001bO000006UJjJQAW',  -- PLAZAS
  -------------- NUEVOS IDS DH
'001bO00000ERXWCQA5', -- COMODIN
'001bO00000ERXZPQA5', -- AMPM - CR
'001bO00000ER97LQAT', -- BIGGIE
'001bO00000ERWv1QAH', -- LUVEBRAS
'001bO00000ERXZQQA5' -- FRESH MARKET
)
THEN 'Small Supermarket'
ELSE 'Supermarket' /*TODO EL RESTO SON SUPERMARKETS*/ END AS clasificacion,
franchise_id
FROM orders
WHERE 
(orders.franchise_id IN (
  '0011r00002VoHgPAAV',	-- Supermercado DAR
'0011r00002VoI9AAAV',	-- Dia (EXCLUIMOS POR EL RANKER)
'0011r00002VoIJ0AAN',	-- Carrefour AR
'001bO000006U6cUQAS', -- Carrefour DO
'0016900002w3vOKAAY',	-- Unicasa
'001690000350YBcAAM',	-- Amarket
'0011r00002VoHxuAAF',	-- Tottus
'0011r00002VoI1SAAV',	-- Super Xtra
'0011r00002VoI77AAF',	-- Super 99
'0011r00002XcCnIAAV',	-- Fidalga
'0011r00002XcDSFAA3',	-- La Anónima
'0016900002aDwwKAAS',	-- Hipermercados Olé
'0016900002aDwxrAAC',	-- Plaza Lama
'0016900002dYyUzAAK',	-- Supermercados Rey
'0016900002dYypxAAC',	-- Supermercado Gran Via
'0016900002f7gZyAAI',	-- Coral Hipermercados
'0016900002f9COxAAM',	-- Supermercados La Torre
'0016900002f9bMlAAI',	-- Santa María
'0016900002fAZOfAAO',	-- Central Madeirense
'0016900002kWsn1AAC',	-- Supermercado Los Jardines
'0016900002ncEiGAAU',	-- Super El Abastecedor
'0016900002uGd9XAAS',	-- DIPROVA
'0016900002uR7uhAAC',	-- Mi Super Fresh
'0016900002w3PPrAAM',	-- Peri
'0016900002w3PYSAA2',	-- Super Compro
'00169000030SXkxAAG',	-- Real Supermercados
'00169000030SXvGAAW',	-- Tata Supermercados
'00169000030Wk7BAAS',	-- La Cadena
'00169000030ZfPNAA0',	-- Sirena
'001bO000006UJjJQAW',  -- Plazas
'001bO00000BWifaQAD',  -- 'La Puerta del Sol'
-------------- NUEVOS IDS DH
'001bO00000ERXW9QAP', -- DISCO
'001bO00000ERXW8QAP', -- JUMBO
'001bO00000ERXWCQA5', -- COMODIN
'001bO00000ERXWAQA5', -- LIBERTAD
'001bO00000ERXZPQA5', -- AMPM - CR
'001bO00000ERXayQAH', -- TIA
'001bO00000ERW2FQAX', -- LA COLONIA
'001bO00000ER97LQAT', -- BIGGIE
'001bO00000ERRX8QAP', -- METRO
'001bO00000ERRX9QAP', -- WONG
'001bO00000ERWv1QAH', -- LUVEBRAS
'001bO00000ERVsWQAX', -- MIXTURA
'001bO00000ERXW7QAP', -- VEA
'001bO00000ERXZQQA5', -- FRESH MARKET
'001bO00000ER97QQAT'  -- TOTTUS NUEVA

)

)
GROUP BY 1,2,3,4,5,6,7
)
SELECT
SUMMARY.*,
case when LOWER(aaa_tag_same_price.TAG_SAME_PRICE) = "same price" THEN TRUE else FALSE END is_same_price
FROM SUMMARY
LEFT JOIN `peya-data-origins-pro.raw_qcommerce.gsheet_aaa_tag_same_price` AS aaa_tag_same_price
ON LOWER(franchise) = LOWER(vendor) and SUMMARY.cc = aaa_tag_same_price.country