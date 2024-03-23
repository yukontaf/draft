SELECT
    properties.variant AS variant
    , SAFE_CAST(c.user_id AS INT64) AS user_id
FROM `analytics-147612`.`bloomreach_raw`.`campaign` c
-- left join (select user_id, properties.campaign_trigger as field from `analytics-147612`.`bloomreach_raw`.`campaign`
-- ) p on c.user_id = p.user_id
WHERE campaign_id = '65cdead01d9a4d20fdcccd69'
AND action_id = 74

SELECT
    user_id
     , variant
--      , DATE_TRUNC(open_time_dt, DAY) AS deal_day
--      , symbol_name
     , COUNT(DISTINCT operation_id) AS deals_cnt
     , SUM(volume) AS symbol_volume
     , SUM(CASE WHEN symbol_name = 'ETHUSD' THEN volume ELSE 0 END) as eth_vol
     , MIN(CASE WHEN symbol_name = 'ETHUSD' THEN open_time_dt ELSE NULL END) as first_eth_date
     , SUM(CASE WHEN symbol_name = 'ETHUSD' THEN 1 ELSE 0 END) as eth_deals_cnt
     , IF(SUM(CASE WHEN symbol_name = 'ETHUSD' THEN 1 ELSE 0 END) > 0, 1, 0) AS converted
FROM
    dev_gsokolov.user_deals
WHERE
    DATE(close_time_dt) BETWEEN '2024-03-04' AND '2024-03-10'
-- AND DATE(close_time_dt) BETWEEN '2024-02-26' AND '2024-03-04'
    AND variant is not null
GROUP BY
    user_id, variant