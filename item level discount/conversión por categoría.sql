SELECT
  level_one,
  level_two,
  level_three,
  SUM(any_click) as clicks,
  SUM(confirmed_orders) as orders,
  SAFE_DIVIDE(SUM(confirmed_orders),SUM(any_click)) as conversion
FROM
  `peya-bi-tools-pro.il_qcommerce.popularity` pop
INNER JOIN  
  `automated_tables_reports.partners_aaa_temporary` aaa
ON
  aaa.partner_id = pop.partner_id 
AND
  pop.date BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 1 MONTH) AND CURRENT_DATE()
AND
  aaa.franchise = 'Ta-Ta-UY'
AND
  user_segment IN ('new','prospect')
GROUP BY
  ALL