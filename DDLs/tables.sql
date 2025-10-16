-- ===============================================================
--  Athena DDLs for Agentic FinOps (Project Tokyo)
-- ===============================================================

-- ---------------------------------------------------------------
-- 0) Create Databases
-- ---------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS synthetic_cur;
CREATE DATABASE IF NOT EXISTS cost_comparison;

-- ---------------------------------------------------------------
-- 1) RAW COST & USAGE REPORT TABLE
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS synthetic_cur.raw;
CREATE EXTERNAL TABLE synthetic_cur.raw(
  bill_billing_period_start_date string COMMENT 'from deserializer',
  line_item_usage_start_date     string COMMENT 'from deserializer',
  line_item_usage_end_date       string COMMENT 'from deserializer',
  line_item_product_code         string COMMENT 'from deserializer',
  line_item_usage_type           string COMMENT 'from deserializer',
  line_item_operation            string COMMENT 'from deserializer',
  product_region                 string COMMENT 'from deserializer',
  line_item_resource_id          string COMMENT 'from deserializer',
  line_item_unblended_cost       string COMMENT 'from deserializer',
  line_item_blended_cost         string COMMENT 'from deserializer',
  line_item_unblended_rate       string COMMENT 'from deserializer',
  pricing_currency               string COMMENT 'from deserializer',
  line_item_usage_amount         string COMMENT 'from deserializer',
  resource_tags_user_env         string COMMENT 'from deserializer',
  resource_tags_user_app         string COMMENT 'from deserializer',
  identity_linked_account_id     string COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=','
)
STORED AS TEXTFILE
LOCATION 's3://cur-data-agentic-ai/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---------------------------------------------------------------
-- 2) RECOMMENDATIONS TABLE
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS synthetic_cur.recommendations_v2;
CREATE EXTERNAL TABLE synthetic_cur.recommendations_v2(
  run_id                 string COMMENT 'from deserializer',
  created_at             string COMMENT 'from deserializer',
  category               string COMMENT 'from deserializer',
  subtype                string COMMENT 'from deserializer',
  region                 string COMMENT 'from deserializer',
  assumption             string COMMENT 'from deserializer',
  metric                 string COMMENT 'from deserializer',
  est_monthly_saving_usd string COMMENT 'from deserializer',
  one_time_saving_usd    string COMMENT 'from deserializer',
  action_sql_hint        string COMMENT 'from deserializer',
  source_note            string COMMENT 'from deserializer',
  rline_item_resource_id string COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=','
)
STORED AS TEXTFILE
LOCATION 's3://athena-query-results-agentic-ai/cost-agent-v2/recommendations'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---------------------------------------------------------------
-- 3) FORECAST DAILY COST TABLE (PARTITIONED BY run_id)
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS synthetic_cur.forecast_daily_csv_raw;
CREATE EXTERNAL TABLE synthetic_cur.forecast_daily_csv_raw(
  ds         string COMMENT 'forecast date',
  yhat       string COMMENT 'predicted mean',
  yhat_lower string COMMENT 'lower bound',
  yhat_upper string COMMENT 'upper bound'
)
PARTITIONED BY (run_id string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=','
)
STORED AS TEXTFILE
LOCATION 's3://athena-query-results-agentic-ai/cost-agent-v2/forecast_table'
TBLPROPERTIES ('skip.header.line.count'='1');
-- After new runs, execute:
--   MSCK REPAIR TABLE synthetic_cur.forecast_daily_csv_raw;

-- ---------------------------------------------------------------
-- 4) PRICING RAW TABLE (MULTI-CLOUD CSV SOURCE)
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS cost_comparison.pricing_raw;
CREATE EXTERNAL TABLE cost_comparison.pricing_raw(
  provider           string COMMENT 'from deserializer',
  service_category   string COMMENT 'from deserializer',
  service_name       string COMMENT 'from deserializer',
  sku_hint           string COMMENT 'from deserializer',
  price_usd          string COMMENT 'from deserializer',
  unit_raw           string COMMENT 'from deserializer',
  unit_standardized  string COMMENT 'from deserializer',
  price_period       string COMMENT 'from deserializer',
  region             string COMMENT 'from deserializer',
  pricing_link       string COMMENT 'from deserializer',
  source_notes       string COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=','
)
STORED AS TEXTFILE
LOCATION 's3://cross-cloud-comparison/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---------------------------------------------------------------
-- 5) NORMALIZED CROSS-CLOUD OFFERINGS TABLE (PARQUET)
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS cost_comparison.cross_cloud_offerings_new;
CREATE TABLE cost_comparison.cross_cloud_offerings_new
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
) AS
WITH cleaned AS (
  SELECT
    TRIM(provider)                                  AS provider,
    TRIM(service_category)                          AS service_category,
    TRIM(service_name)                              AS service_name,
    NULLIF(TRIM(sku_hint), '')                      AS sku_hint,
    TRIM(region)                                    AS region,
    TRY_CAST(REGEXP_REPLACE(price_usd, '[^0-9.\\-]', '') AS DECIMAL(18,6)) AS price_usd,
    LOWER(TRIM(unit_standardized))                  AS unit_standardized,
    LOWER(TRIM(price_period))                       AS price_period,
    NULLIF(TRIM(pricing_link), '')                  AS pricing_link,
    NULLIF(TRIM(source_notes), '')                  AS source_notes
  FROM cost_comparison.pricing_raw
)
SELECT * FROM cleaned
WHERE price_usd IS NOT NULL;

-- ---------------------------------------------------------------
-- 6) COST ANOMALIES TABLE (OPTIONAL PERSISTENCE)
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS synthetic_cur.cost_anomalies;
CREATE EXTERNAL TABLE synthetic_cur.cost_anomalies(
  usage_date        date,
  account_id        string,
  service           string,
  region            string,
  cost              double,
  robust_z          double,
  ewma_z            double,
  iqr_flag          boolean,
  ensemble_votes    int,
  severity          double,
  explanation_seed  string,
  explanation       string,
  run_id            string,
  created_at        string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://athena-query-results-agentic-ai/cost-agent-v2/anomalies'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ===============================================================
--  End of Athena table definitions for Agentic FinOps
-- ===============================================================
