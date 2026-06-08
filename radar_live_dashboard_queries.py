# Databricks Dashboard Queries
# These queries power the Radar Live SLA Monitoring Dashboard
# Each query maps to a specific dashboard widget

# ================================================================
# WIDGET 1 — SYSTEM HEALTH OVERVIEW
# One row per source showing overall health status
# ================================================================

widget_1_system_health = """
WITH date_range AS (
  SELECT
    CAST('{{ start_date }}' AS TIMESTAMP) AS from_ts,
    CAST('{{ end_date }}'   AS TIMESTAMP) AS to_ts
),
ehub_health AS (
  SELECT
    'EHUB' AS source,
    COUNT(DISTINCT interval_end)                                                          AS total_intervals,
    COUNT(DISTINCT CASE WHEN is_sla_breach = 1 THEN interval_end END)                    AS breached_intervals,
    SUM(message_count)                                                                    AS total_messages,
    ROUND(100.0 * COUNT(CASE WHEN is_sla_breach = 0 THEN 1 END) /
          NULLIF(COUNT(*), 0), 1)                                                         AS health_pct,
    MAX(interval_end)                                                                     AS latest_interval
  FROM core_bld.default.monitoring_ehub_30min_update
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND message_type <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
),
bronze_health AS (
  SELECT
    'BRONZE' AS source,
    COUNT(DISTINCT interval_end)                                                          AS total_intervals,
    COUNT(DISTINCT CASE WHEN is_sla_breach = 1 THEN interval_end END)                    AS breached_intervals,
    SUM(message_count)                                                                    AS total_messages,
    ROUND(100.0 * COUNT(CASE WHEN is_sla_breach = 0 THEN 1 END) /
          NULLIF(COUNT(*), 0), 1)                                                         AS health_pct,
    MAX(interval_end)                                                                     AS latest_interval
  FROM core_bld.default.monitoring_mqs_bronze_30min_update
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND message_type <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
),
interaction_health AS (
  SELECT
    'INTERACTION' AS source,
    COUNT(DISTINCT interval_end)                                                          AS total_intervals,
    COUNT(DISTINCT CASE WHEN is_sla_breach = 1 THEN interval_end END)                    AS breached_intervals,
    SUM(total_count)                                                                      AS total_messages,
    ROUND(100.0 * COUNT(CASE WHEN is_sla_breach = 0 THEN 1 END) /
          NULLIF(COUNT(*), 0), 1)                                                         AS health_pct,
    MAX(interval_end)                                                                     AS latest_interval
  FROM core_bld.default.monitoring_mqs_interaction_status
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND status = 'ALL'
    AND brand IN ('AZ','BRITANNIA')
)
SELECT
  source,
  total_intervals,
  breached_intervals,
  total_intervals - breached_intervals                                                    AS healthy_intervals,
  total_messages,
  health_pct,
  CASE
    WHEN health_pct >= 95 THEN 'OK'
    WHEN health_pct >= 80 THEN 'MINOR'
    WHEN health_pct >= 60 THEN 'MEDIUM'
    ELSE 'HIGH'
  END                                                                                     AS alert_level,
  latest_interval
FROM (
  SELECT * FROM ehub_health
  UNION ALL
  SELECT * FROM bronze_health
  UNION ALL
  SELECT * FROM interaction_health
)
ORDER BY source
"""

# ================================================================
# WIDGET 2 — ALERT LEVEL PER BRAND PER INTERVAL
# Drives the heatmap — one row per (interval, brand, source)
# ================================================================

widget_2_alert_level_by_interval = """
WITH intervals AS (
  SELECT
    interval_end,
    brand,
    COUNT(DISTINCT message_type)                                                          AS total_types,
    COUNT(DISTINCT CASE WHEN is_sla_breach = 1 THEN message_type END)                    AS breached_types,
    SUM(message_count)                                                                    AS total_count,
    MAX(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END)                                  AS any_breach
  FROM core_bld.default.monitoring_ehub_30min_update
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND message_type <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
  GROUP BY interval_end, brand
),
with_consecutive AS (
  SELECT
    interval_end,
    brand,
    total_types,
    breached_types,
    total_count,
    any_breach,
    -- Consecutive zero count using window function
    SUM(CASE WHEN breached_types = total_types THEN 1 ELSE 0 END)
      OVER (
        PARTITION BY brand
        ORDER BY interval_end
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) -
    COALESCE(
      MAX(CASE WHEN breached_types < total_types
               THEN SUM(CASE WHEN breached_types = total_types THEN 1 ELSE 0 END)
                      OVER (PARTITION BY brand ORDER BY interval_end
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          END)
        OVER (PARTITION BY brand ORDER BY interval_end
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    )                                                                                     AS consecutive_zeros
  FROM intervals
),
with_alert AS (
  SELECT
    interval_end,
    brand,
    total_types,
    breached_types,
    total_count,
    any_breach,
    GREATEST(0, consecutive_zeros)                                                        AS consecutive_zeros,
    -- Alert by message types impacted
    CASE
      WHEN breached_types >= {{ high_threshold }}   THEN 3
      WHEN breached_types >= {{ medium_threshold }} THEN 2
      WHEN breached_types >= {{ minor_threshold }}  THEN 1
      ELSE 0
    END                                                                                   AS level_by_types,
    -- Alert by consecutive zeros (1=Minor, 2-3=Medium, 4+=High)
    CASE
      WHEN GREATEST(0, consecutive_zeros) >= 4 THEN 3
      WHEN GREATEST(0, consecutive_zeros) >= 2 THEN 2
      WHEN GREATEST(0, consecutive_zeros) >= 1 THEN 1
      ELSE 0
    END                                                                                   AS level_by_consec
  FROM with_consecutive
)
SELECT
  interval_end,
  DATE_FORMAT(interval_end, 'HH:mm')                                                     AS interval_time,
  brand,
  total_types,
  breached_types,
  total_count,
  any_breach,
  consecutive_zeros,
  GREATEST(level_by_types, level_by_consec)                                               AS alert_level_num,
  CASE GREATEST(level_by_types, level_by_consec)
    WHEN 0 THEN 'OK'
    WHEN 1 THEN 'MINOR'
    WHEN 2 THEN 'MEDIUM'
    WHEN 3 THEN 'HIGH'
  END                                                                                     AS alert_level
FROM with_alert
ORDER BY interval_end, brand
"""

# ================================================================
# WIDGET 3 — EHUB MESSAGE TYPE DETAIL HEATMAP
# One row per (interval, brand, message_type)
# ================================================================

widget_3_ehub_message_type_detail = """
WITH base AS (
  SELECT
    interval_end,
    DATE_FORMAT(interval_end, 'HH:mm')                                                   AS interval_time,
    brand,
    message_type,
    message_count,
    is_sla_breach,
    -- Consecutive zeros per message_type + brand
    SUM(is_sla_breach)
      OVER (
        PARTITION BY brand, message_type
        ORDER BY interval_end
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) -
    COALESCE(
      MAX(CASE WHEN is_sla_breach = 0
               THEN SUM(is_sla_breach)
                      OVER (PARTITION BY brand, message_type
                            ORDER BY interval_end
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          END)
        OVER (PARTITION BY brand, message_type
              ORDER BY interval_end
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    )                                                                                     AS consecutive_breach
  FROM core_bld.default.monitoring_ehub_30min_update
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND message_type <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
)
SELECT
  interval_end,
  interval_time,
  brand,
  message_type,
  message_count,
  is_sla_breach,
  GREATEST(0, consecutive_breach)                                                         AS consecutive_breach,
  CASE
    WHEN is_sla_breach = 0 THEN 'OK'
    WHEN GREATEST(0, consecutive_breach) >= 4 THEN 'HIGH'
    WHEN GREATEST(0, consecutive_breach) >= 2 THEN 'MEDIUM'
    ELSE 'MINOR'
  END                                                                                     AS alert_level
FROM base
ORDER BY interval_end, brand, message_type
"""

# ================================================================
# WIDGET 4 — BRONZE MESSAGE TYPE DETAIL
# Same pattern as EHUB but from bronze table
# ================================================================

widget_4_bronze_detail = """
WITH base AS (
  SELECT
    interval_end,
    DATE_FORMAT(interval_end, 'HH:mm')                                                   AS interval_time,
    brand,
    message_type,
    message_count,
    is_sla_breach,
    SUM(is_sla_breach)
      OVER (
        PARTITION BY brand, message_type
        ORDER BY interval_end
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) -
    COALESCE(
      MAX(CASE WHEN is_sla_breach = 0
               THEN SUM(is_sla_breach)
                      OVER (PARTITION BY brand, message_type
                            ORDER BY interval_end
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          END)
        OVER (PARTITION BY brand, message_type
              ORDER BY interval_end
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    )                                                                                     AS consecutive_breach
  FROM core_bld.default.monitoring_mqs_bronze_30min_update
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND message_type <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
)
SELECT
  interval_end,
  interval_time,
  brand,
  message_type,
  message_count,
  is_sla_breach,
  GREATEST(0, consecutive_breach)                                                         AS consecutive_breach,
  CASE
    WHEN is_sla_breach = 0 THEN 'OK'
    WHEN GREATEST(0, consecutive_breach) >= 4 THEN 'HIGH'
    WHEN GREATEST(0, consecutive_breach) >= 2 THEN 'MEDIUM'
    ELSE 'MINOR'
  END                                                                                     AS alert_level
FROM base
ORDER BY interval_end, brand, message_type
"""

# ================================================================
# WIDGET 5 — INTERACTION STATUS DETAIL
# ================================================================

widget_5_interaction_detail = """
WITH base AS (
  SELECT
    interval_end,
    DATE_FORMAT(interval_end, 'HH:mm')                                                   AS interval_time,
    brand,
    status,
    total_count,
    is_sla_breach,
    SUM(is_sla_breach)
      OVER (
        PARTITION BY brand, status
        ORDER BY interval_end
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) -
    COALESCE(
      MAX(CASE WHEN is_sla_breach = 0
               THEN SUM(is_sla_breach)
                      OVER (PARTITION BY brand, status
                            ORDER BY interval_end
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          END)
        OVER (PARTITION BY brand, status
              ORDER BY interval_end
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    )                                                                                     AS consecutive_breach
  FROM core_bld.default.monitoring_mqs_interaction_status
  WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                     AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
    AND status <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
)
SELECT
  interval_end,
  interval_time,
  brand,
  status,
  total_count,
  is_sla_breach,
  GREATEST(0, consecutive_breach)                                                         AS consecutive_breach,
  CASE
    WHEN is_sla_breach = 0 THEN 'OK'
    WHEN GREATEST(0, consecutive_breach) >= 4 THEN 'HIGH'
    WHEN GREATEST(0, consecutive_breach) >= 2 THEN 'MEDIUM'
    ELSE 'MINOR'
  END                                                                                     AS alert_level
FROM base
ORDER BY interval_end, brand, status
"""

# ================================================================
# WIDGET 6 — OVERALL ALERT LEVEL SUMMARY (KPI COUNTERS)
# Single row — drives the top KPI tiles
# ================================================================

widget_6_kpi_summary = """
WITH date_range AS (
  SELECT
    DATE(CAST('{{ start_date }}' AS TIMESTAMP)) AS from_date,
    DATE(CAST('{{ end_date }}'   AS TIMESTAMP)) AS to_date
),
ehub_intervals AS (
  SELECT
    interval_end,
    brand,
    COUNT(DISTINCT message_type)                                                          AS total_types,
    COUNT(DISTINCT CASE WHEN is_sla_breach = 1 THEN message_type END)                    AS breached_types,
    SUM(message_count)                                                                    AS total_messages
  FROM core_bld.default.monitoring_ehub_30min_update
  WHERE run_date BETWEEN (SELECT from_date FROM date_range)
                     AND (SELECT to_date   FROM date_range)
    AND message_type <> 'ALL'
    AND brand IN ('AZ','BRITANNIA')
  GROUP BY interval_end, brand
),
consec_calc AS (
  SELECT
    brand,
    interval_end,
    breached_types,
    total_types,
    total_messages,
    -- Running consecutive full-breach counter
    CASE WHEN breached_types = total_types THEN
      ROW_NUMBER() OVER (PARTITION BY brand ORDER BY interval_end)
      - MAX(CASE WHEN breached_types < total_types
                 THEN ROW_NUMBER() OVER (PARTITION BY brand ORDER BY interval_end)
            END)
          OVER (PARTITION BY brand ORDER BY interval_end
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    ELSE 0 END                                                                            AS consecutive_zeros
  FROM ehub_intervals
)
SELECT
  COUNT(DISTINCT interval_end)                                                            AS total_intervals,
  COUNT(DISTINCT CASE WHEN breached_types > 0 THEN interval_end END)                     AS breached_intervals,
  MAX(GREATEST(0, consecutive_zeros))                                                     AS max_consecutive_zeros,
  MAX(breached_types)                                                                     AS max_breached_types,
  SUM(total_messages)                                                                     AS total_messages,
  ROUND(100.0 * COUNT(CASE WHEN breached_types = 0 THEN 1 END) /
        NULLIF(COUNT(*), 0), 1)                                                           AS overall_health_pct,
  CASE
    WHEN MAX(breached_types) >= {{ high_threshold }}   OR MAX(GREATEST(0,consecutive_zeros)) >= 4 THEN 'HIGH'
    WHEN MAX(breached_types) >= {{ medium_threshold }} OR MAX(GREATEST(0,consecutive_zeros)) >= 2 THEN 'MEDIUM'
    WHEN MAX(breached_types) >= {{ minor_threshold }}  OR MAX(GREATEST(0,consecutive_zeros)) >= 1 THEN 'MINOR'
    ELSE 'OK'
  END                                                                                     AS overall_alert_level
FROM consec_calc
"""

# ================================================================
# WIDGET 7 — BRAND COMPARISON SIDE BY SIDE
# AZ vs BRITANNIA health over time
# ================================================================

widget_7_brand_comparison = """
SELECT
  interval_end,
  DATE_FORMAT(interval_end, 'HH:mm')                                                     AS interval_time,
  brand,
  SUM(message_count)                                                                      AS total_count,
  COUNT(DISTINCT CASE WHEN is_sla_breach = 1 THEN message_type END)                      AS breached_types,
  COUNT(DISTINCT message_type)                                                            AS total_types,
  ROUND(100.0 * COUNT(CASE WHEN is_sla_breach = 0 THEN 1 END) /
        NULLIF(COUNT(*), 0), 1)                                                           AS health_pct,
  MAX(is_sla_breach)                                                                      AS any_breach
FROM core_bld.default.monitoring_ehub_30min_update
WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                   AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
  AND message_type <> 'ALL'
  AND brand IN ('AZ','BRITANNIA')
GROUP BY interval_end, brand
ORDER BY interval_end, brand
"""

# ================================================================
# WIDGET 8 — TOP BREACHED MESSAGE TYPES
# Ranked by breach frequency across the period
# ================================================================

widget_8_top_breaches = """
SELECT
  brand,
  message_type,
  COUNT(*)                                                                                AS total_intervals,
  SUM(is_sla_breach)                                                                      AS breach_count,
  ROUND(100.0 * SUM(is_sla_breach) / NULLIF(COUNT(*), 0), 1)                            AS breach_pct,
  SUM(message_count)                                                                      AS total_messages,
  CASE
    WHEN ROUND(100.0 * SUM(is_sla_breach) / NULLIF(COUNT(*), 0), 1) >= 50 THEN 'HIGH'
    WHEN ROUND(100.0 * SUM(is_sla_breach) / NULLIF(COUNT(*), 0), 1) >= 20 THEN 'MEDIUM'
    WHEN ROUND(100.0 * SUM(is_sla_breach) / NULLIF(COUNT(*), 0), 1) >  0  THEN 'MINOR'
    ELSE 'OK'
  END                                                                                     AS alert_level
FROM core_bld.default.monitoring_ehub_30min_update
WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                   AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
  AND message_type <> 'ALL'
  AND brand IN ('AZ','BRITANNIA')
GROUP BY brand, message_type
ORDER BY breach_pct DESC, breach_count DESC
"""

# ================================================================
# WIDGET 9 — UNEXPECTED BRAND TRAFFIC
# Shows OTHER brand activity — needs investigation
# ================================================================

widget_9_unexpected_brand = """
SELECT
  interval_end,
  DATE_FORMAT(interval_end, 'HH:mm')                                                     AS interval_time,
  message_type,
  message_count,
  is_unexpected_brand
FROM core_bld.default.monitoring_ehub_30min_update
WHERE run_date BETWEEN DATE(CAST('{{ start_date }}' AS TIMESTAMP))
                   AND DATE(CAST('{{ end_date }}'   AS TIMESTAMP))
  AND brand = 'OTHER'
  AND message_count > 0
ORDER BY interval_end DESC, message_count DESC
LIMIT 100
"""

# ================================================================
# WIDGET 10 — ALERT STATE HISTORY
# Shows when alerts were sent and recovered
# ================================================================

widget_10_alert_history = """
SELECT
  alert_name,
  current_status,
  notified_status,
  consecutive_count,
  last_interval,
  last_notified_at,
  CASE current_status
    WHEN 'BREACH'  THEN 'HIGH'
    WHEN 'HEALTHY' THEN 'OK'
    ELSE 'UNKNOWN'
  END                                                                                     AS alert_level
FROM core_bld.default.sla_alert_state
ORDER BY last_notified_at DESC
"""

print("Dashboard queries loaded successfully.")
print(f"Total widgets defined: 10")
