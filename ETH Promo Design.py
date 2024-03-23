import warnings
import pandas as pd
import pandas_gbq as gbq
import seaborn as sns
from google.cloud import bigquery

client = bigquery.Client()
pal = [
    "#be0707",
    "#dc3248",
    "#ef5a80",
    "#f982b2",
    "#fca9dd",
    "#ffcfff",
    "#eebdfb",
    "#daacf9",
    "#c19cf8",
    "#a48ef7",
    "#7e82f7",
]
project = "analytics-147612"
location = "EU"
client = bigquery.Client(project=project, location=location)


def table_reference(dataset, table):
    return f"{project}.{dataset}.{table}"


def get_table_fields(dataset, table):
    table_ref = client.get_table(table_reference(dataset, table))
    with open(f"/Users/gsokolov/Documents/{table}_fields.txt", "w") as f:
        data = {
            "table_name": table_reference(dataset, table),
            "fields": [field.name for field in table_ref.schema],
        }
    return print(data)


user_ids = pd.read_csv(
    "/Users/gsokolov/Library/CloudStorage/GoogleDrive-gsokolov@ourgapps.com/My Drive/Exported Data/ETH_recom_280224.csv"
)

random_user_ids = pd.read_csv(
    "/Users/gsokolov/Library/CloudStorage/GoogleDrive-gsokolov@ourgapps.com/My Drive/Exported Data/Random_users_280224.csv"
)

sql = "\nSELECT * FROM dev_gsokolov.user_deals\nLIMIT 1000\n"
deals = gbq.read_gbq(sql, project_id="analytics-147612", location="EU")


user_deals_cnt = pd.read_gbq(
    "\nSELECT\n    ud.user_id\n    , SUM(ud.deals_cnt) AS total_deals_cnt\n    , SUM(COALESCE(uc.eth_deals_cnt, 0)) AS eth_deals_cnt\nFROM dev_gsokolov.user_conversion AS uc\n    RIGHT JOIN dev_gsokolov.user_deals AS ud\n        ON uc.user_id = ud.user_id\nGROUP BY ud.user_id\nORDER BY eth_deals_cnt DESC;\n"
)

user_eth_stats = pd.read_gbq("\nSELECT * FROM dev_gsokolov.user_conversion\n")

user_stats = pd.read_gbq(
    "\nSELECT\n    ud.user_id\n    , ud.trade_day\n    , COUNT(DISTINCT ud.operation_id) AS deals_cnt\n    , SUM(ud.volume) AS sum_vol\n    , SUM(ud.profit) AS sum_profit\n    , AVG(\n        CAST(TIMESTAMP_DIFF(ud.close_time_dt, ud.open_time_dt, SECOND) / 60 AS INT64)\n    ) AS avg_deal_duration\nFROM dev_gsokolov.user_deals_flat AS ud\n    LEFT JOIN dev_gsokolov.user_conversion AS uc\n        ON ud.user_id = uc.user_id\nGROUP BY\n    1, 2\n"
)


avg_eth_vol = (
    user_eth_stats.groupby("user_id")["eth_sum_vol"].sum()
    / user_eth_stats.groupby("user_id")["eth_deals_cnt"].sum()
)

"""
%%sql
select * from dev_gsokolov.user_deals_flat
"""

random_user_deals = pd.read_gbq()

grouped_by_user_and_day = random_user_deals.groupby(["user_id", "trade_day"])
deals_count = (
    grouped_by_user_and_day["operation_id"].nunique().reset_index(name="total_deals")
)
volume_sum = grouped_by_user_and_day["volume"].sum().reset_index(name="total_volume")
result = pd.merge(deals_count, volume_sum, on=["user_id", "trade_day"])
result

ethusd_df = random_user_deals[random_user_deals["symbol_name"] == "ETHUSD"]
ethusd_grouped_by_user_and_day = ethusd_df.groupby(["user_id", "trade_day"])
ethusd_deals_count = (
    ethusd_grouped_by_user_and_day["operation_id"]
    .nunique()
    .reset_index(name="ethusd_total_deals")
)
ethusd_volume_sum = (
    ethusd_grouped_by_user_and_day["volume"]
    .sum()
    .reset_index(name="ethusd_total_volume")
)
ethusd_result = pd.merge(
    ethusd_deals_count, ethusd_volume_sum, on=["user_id", "trade_day"]
)
ethusd_result

total_deals = random_user_deals["operation_id"].nunique()
ethusd_deals = random_user_deals[random_user_deals["symbol_name"] == "ETHUSD"][
    "operation_id"
].nunique()
percent_ethusd_deals = ethusd_deals / total_deals * 100
unique_ethusd_users = random_user_deals[random_user_deals["symbol_name"] == "ETHUSD"][
    "user_id"
].nunique()
percent_unique_ethusd_users = (
    unique_ethusd_users / random_user_deals["user_id"].nunique() * 100
)

"""
%%sql
WITH users_all AS (
    SELECT DISTINCT user_id
    FROM (
        SELECT *
        FROM dev_gsokolov.eth_recom
        UNION ALL
        SELECT * FROM
            dev_gsokolov.random_users
    )
)

SELECT
    DATE(DATE_TRUNC(
        open_time_dt,
        WEEK (MONDAY)
    )) AS trade_week,
    COUNT(DISTINCT user_id) AS user_cnt,
    COUNT(DISTINCT operation_id) AS deal_cnt
FROM wh_raw.trading_real_raw
WHERE
    DATE(open_time_dt) BETWEEN '2024-01-01' AND '2024-03-01'
    AND user_id IN (SELECT user_id FROM users_all)
GROUP BY 1
ORDER BY 1 DESC
"""

"""
%%sql
SELECT
    properties.variant AS variant
    , SAFE_CAST(c.user_id AS INT64) AS user_id
FROM `analytics-147612`.`bloomreach_raw`.`campaign` c
WHERE campaign_id = '65cdead01d9a4d20fdcccd69'
AND action_id = 74
"""

"""
%%sql
select *
from dev_gsokolov.ab_users
"""

"""
%%sql
select * from
             dev_gsokolov.user_deals
"""

ab_stats = pd.read_gbq("""
SELECT
    ab.user_id
     , ab.variant
--      , DATE_TRUNC(d.open_time_dt, DAY) AS deal_day
--      , d.symbol_name
     , COUNT(DISTINCT d.operation_id) AS deals_cnt
     , SUM(d.volume) AS symbol_volume
     , SUM(CASE WHEN d.symbol_name = 'ETHUSD' THEN d.volume ELSE 0 END) as eth_vol
     , MIN(CASE WHEN d.symbol_name = 'ETHUSD' THEN d.open_time_dt ELSE NULL END) as first_eth_date
     , SUM(CASE WHEN d.symbol_name = 'ETHUSD' THEN 1 ELSE 0 END) as eth_deals_cnt
     , IF(SUM(CASE WHEN d.symbol_name = 'ETHUSD' THEN 1 ELSE 0 END) > 0, 1, 0) AS converted
FROM
    dev_gsokolov.ab_users ab
LEFT JOIN dev_gsokolov.user_deals d
ON ab.user_id = d.user_id
AND DATE(d.close_time_dt) BETWEEN '2024-03-05' AND '2024-03-10'
-- AND DATE(d.close_time_dt) BETWEEN '2024-02-26' AND '2024-03-04'
AND d.variant is not null
GROUP BY
    ab.user_id, ab.variant
"""
)

ab_stats_filtered = ab_stats[ab_stats["deals_cnt"] > 0]

grouped_stats = (
    ab_stats.groupby("variant")
    .agg(
        user_count=pd.NamedAgg(column="user_id", aggfunc="nunique"),
        avg_vol_ethusd=pd.NamedAgg(column="eth_vol", aggfunc="mean"),
        std_volume_ethusd=pd.NamedAgg(column="eth_vol", aggfunc=lambda x: x.std()),
        total_deals_eth=pd.NamedAgg(column="eth_deals_cnt", aggfunc="sum"),
        vol_eth=pd.NamedAgg(column="eth_vol", aggfunc="sum"),
        total_vol=pd.NamedAgg(column="symbol_volume", aggfunc="sum"),
        total_converted=pd.NamedAgg(column="converted", aggfunc="sum"),
        total_deals=pd.NamedAgg(column="deals_cnt", aggfunc="sum"),
    )
    .round(4)
)
grouped_stats_filtered = (
    ab_stats_filtered.groupby("variant")
    .agg(
        user_count=pd.NamedAgg(column="user_id", aggfunc="nunique"),
        avg_vol_ethusd=pd.NamedAgg(column="eth_vol", aggfunc="mean"),
        std_volume_ethusd=pd.NamedAgg(column="eth_vol", aggfunc=lambda x: x.std()),
        total_deals_eth=pd.NamedAgg(column="eth_deals_cnt", aggfunc="sum"),
        vol_eth=pd.NamedAgg(column="eth_vol", aggfunc="sum"),
        total_vol=pd.NamedAgg(column="symbol_volume", aggfunc="sum"),
        total_converted=pd.NamedAgg(column="converted", aggfunc="sum"),
        total_deals=pd.NamedAgg(column="deals_cnt", aggfunc="sum"),
    )
    .round(4)
)
