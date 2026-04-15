-- ============================================================
-- Cyclistic Bike-Share Analysis
-- Google Data Analytics Certificate Capstone | March 2025
-- Author: Donovan Corbin
-- Tools: SQL (compatible with BigQuery / PostgreSQL)
-- Description: Replicates and extends the R-based analysis
--   using SQL to explore behavioral differences between
--   casual riders and annual members across Q1 2019-2020.
-- ============================================================


-- ============================================================
-- STEP 1: DATA EXPLORATION
-- Understand structure, volume, and data quality before cleaning
-- ============================================================

-- Row counts per dataset
SELECT 'q1_2019' AS dataset, COUNT(*) AS row_count FROM divvy_trips_2019_q1
UNION ALL
SELECT 'q1_2020' AS dataset, COUNT(*) AS row_count FROM divvy_trips_2020_q1;
-- Expected: 365,069 (2019) + 426,887 (2020) = 791,956 total

-- Preview Q1 2019 structure
SELECT *
FROM divvy_trips_2019_q1
LIMIT 5;

-- Preview Q1 2020 structure
SELECT *
FROM divvy_trips_2020_q1
LIMIT 5;

-- Check distinct member/user type labels in each dataset
-- Q1 2019 used "Subscriber" and "Customer"
-- Q1 2020 used "member" and "casual"
SELECT usertype, COUNT(*) AS count
FROM divvy_trips_2019_q1
GROUP BY usertype;

SELECT member_casual, COUNT(*) AS count
FROM divvy_trips_2020_q1
GROUP BY member_casual;

-- Check for negative ride durations (data quality issue)
SELECT COUNT(*) AS negative_duration_count
FROM divvy_trips_2019_q1
WHERE (CAST(end_time AS TIMESTAMP) - CAST(start_time AS TIMESTAMP)) < INTERVAL '0 seconds';

SELECT COUNT(*) AS negative_duration_count
FROM divvy_trips_2020_q1
WHERE (CAST(ended_at AS TIMESTAMP) - CAST(started_at AS TIMESTAMP)) < INTERVAL '0 seconds';

-- Check for QC/maintenance rides (bikes removed from docks by Divvy staff)
SELECT COUNT(*) AS qc_ride_count
FROM divvy_trips_2019_q1
WHERE from_station_name = 'HQ QR';


-- ============================================================
-- STEP 2: COMBINE AND STANDARDIZE
-- Rename 2019 columns to match 2020 schema, then union
-- ============================================================

CREATE OR REPLACE VIEW all_trips AS

  -- Q1 2019: rename columns to match 2020 schema, standardize labels
  SELECT
    CAST(trip_id AS VARCHAR)        AS ride_id,
    CAST(bikeid AS VARCHAR)         AS rideable_type,
    CAST(start_time AS TIMESTAMP)   AS started_at,
    CAST(end_time AS TIMESTAMP)     AS ended_at,
    from_station_name               AS start_station_name,
    from_station_id                 AS start_station_id,
    to_station_name                 AS end_station_name,
    to_station_id                   AS end_station_id,
    CASE
      WHEN usertype = 'Subscriber' THEN 'member'
      WHEN usertype = 'Customer'   THEN 'casual'
      ELSE usertype
    END                             AS member_casual
  FROM divvy_trips_2019_q1

  UNION ALL

  -- Q1 2020: already uses current schema and labels
  SELECT
    ride_id,
    rideable_type,
    CAST(started_at AS TIMESTAMP)   AS started_at,
    CAST(ended_at AS TIMESTAMP)     AS ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    member_casual
  FROM divvy_trips_2020_q1;

-- Confirm combined row count
SELECT COUNT(*) AS total_rows FROM all_trips;
-- Expected: 791,956


-- ============================================================
-- STEP 3: CLEAN DATA
-- Remove QC rides and negative durations
-- Output: all_trips_clean
-- ============================================================

CREATE OR REPLACE VIEW all_trips_clean AS
SELECT
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  start_station_name,
  start_station_id,
  end_station_name,
  end_station_id,
  member_casual,

  -- Ride length in seconds
  EXTRACT(EPOCH FROM (ended_at - started_at))     AS ride_length_seconds,

  -- Date parts for time-series aggregation
  DATE(started_at)                                 AS ride_date,
  EXTRACT(MONTH FROM started_at)                   AS ride_month,
  EXTRACT(DAY FROM started_at)                     AS ride_day,
  EXTRACT(YEAR FROM started_at)                    AS ride_year,

  -- Day of week (0 = Sunday ... 6 = Saturday in PostgreSQL EXTRACT DOW)
  TO_CHAR(started_at, 'Day')                       AS day_of_week

FROM all_trips
WHERE
  start_station_name != 'HQ QR'                    -- remove Divvy QC rides
  AND ended_at > started_at;                        -- remove negative durations

-- Confirm cleaned row count
SELECT COUNT(*) AS cleaned_rows FROM all_trips_clean;
-- Expected: 788,189 (removed 3,767 invalid records)

-- Confirm label standardization
SELECT member_casual, COUNT(*) AS count
FROM all_trips_clean
GROUP BY member_casual;


-- ============================================================
-- STEP 4: DESCRIPTIVE ANALYSIS
-- Summary statistics on ride_length by customer type
-- ============================================================

-- Overall descriptive stats by rider type
SELECT
  member_casual,
  COUNT(*)                                          AS total_rides,
  ROUND(AVG(ride_length_seconds), 2)               AS avg_ride_length_sec,
  ROUND(AVG(ride_length_seconds) / 60.0, 2)        AS avg_ride_length_min,
  ROUND(MIN(ride_length_seconds), 2)               AS min_ride_length_sec,
  ROUND(MAX(ride_length_seconds), 2)               AS max_ride_length_sec,
  PERCENTILE_CONT(0.5)
    WITHIN GROUP (ORDER BY ride_length_seconds)     AS median_ride_length_sec
FROM all_trips_clean
GROUP BY member_casual
ORDER BY member_casual;

-- Key result:
--   casual | avg ~5,478 sec (~91 min)
--   member | avg ~822 sec  (~14 min)
--   Ratio: casual rides are ~6.7x longer on average


-- ============================================================
-- STEP 5: AVERAGE RIDE LENGTH BY DAY OF WEEK
-- This is the core finding visualized in Tableau
-- ============================================================

SELECT
  member_casual,
  day_of_week,
  COUNT(*)                                          AS number_of_rides,
  ROUND(AVG(ride_length_seconds), 2)               AS avg_ride_length_sec,
  ROUND(AVG(ride_length_seconds) / 60.0, 2)        AS avg_ride_length_min
FROM all_trips_clean
GROUP BY member_casual, day_of_week
ORDER BY
  member_casual,
  CASE day_of_week
    WHEN 'Sunday   ' THEN 1
    WHEN 'Monday   ' THEN 2
    WHEN 'Tuesday  ' THEN 3
    WHEN 'Wednesday' THEN 4
    WHEN 'Thursday ' THEN 5
    WHEN 'Friday   ' THEN 6
    WHEN 'Saturday ' THEN 7
  END;

-- Exact values from analysis:
--   casual | Sunday    | 5,061.3 sec | 84.4 min
--   casual | Monday    | 4,752.1 sec | 79.2 min
--   casual | Tuesday   | 4,561.8 sec | 76.0 min
--   casual | Wednesday | 4,480.4 sec | 74.7 min
--   casual | Thursday  | 8,451.7 sec | 140.9 min  <-- peak
--   casual | Friday    | 6,090.7 sec | 101.5 min
--   casual | Saturday  | 4,950.8 sec | 82.5 min
--   member | all days  | 707 - 974 sec | 11.8 - 16.2 min (low variance)


-- ============================================================
-- STEP 6: RIDE VOLUME BY DAY OF WEEK
-- Members dominate weekday ride counts; reveals commuter pattern
-- ============================================================

SELECT
  member_casual,
  day_of_week,
  COUNT(*) AS number_of_rides
FROM all_trips_clean
GROUP BY member_casual, day_of_week
ORDER BY
  member_casual,
  CASE day_of_week
    WHEN 'Sunday   ' THEN 1
    WHEN 'Monday   ' THEN 2
    WHEN 'Tuesday  ' THEN 3
    WHEN 'Wednesday' THEN 4
    WHEN 'Thursday ' THEN 5
    WHEN 'Friday   ' THEN 6
    WHEN 'Saturday ' THEN 7
  END;


-- ============================================================
-- STEP 7: MONTHLY TRENDS
-- Understand seasonal patterns in Q1 (Jan-Mar)
-- ============================================================

SELECT
  member_casual,
  ride_year,
  ride_month,
  COUNT(*)                                          AS number_of_rides,
  ROUND(AVG(ride_length_seconds), 2)               AS avg_ride_length_sec
FROM all_trips_clean
GROUP BY member_casual, ride_year, ride_month
ORDER BY member_casual, ride_year, ride_month;


-- ============================================================
-- STEP 8: RIDE LENGTH DISTRIBUTION BUCKETS
-- Segment rides by duration to understand usage patterns
-- ============================================================

SELECT
  member_casual,
  CASE
    WHEN ride_length_seconds < 300   THEN 'Under 5 min'
    WHEN ride_length_seconds < 900   THEN '5-15 min'
    WHEN ride_length_seconds < 1800  THEN '15-30 min'
    WHEN ride_length_seconds < 3600  THEN '30-60 min'
    WHEN ride_length_seconds < 7200  THEN '1-2 hours'
    ELSE 'Over 2 hours'
  END                                               AS duration_bucket,
  COUNT(*)                                          AS number_of_rides,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY member_casual), 2) AS pct_of_type
FROM all_trips_clean
GROUP BY member_casual, duration_bucket
ORDER BY member_casual,
  CASE
    WHEN ride_length_seconds < 300   THEN 1
    WHEN ride_length_seconds < 900   THEN 2
    WHEN ride_length_seconds < 1800  THEN 3
    WHEN ride_length_seconds < 3600  THEN 4
    WHEN ride_length_seconds < 7200  THEN 5
    ELSE 6
  END;

-- This query reveals what % of each rider type falls into each duration bucket
-- Useful for identifying high-engagement casual riders (Over 2 hours)
-- who represent the strongest membership conversion targets


-- ============================================================
-- STEP 9: EXPORT SUMMARY FOR VISUALIZATION
-- Final aggregation matching the Tableau output
-- ============================================================

SELECT
  member_casual                                     AS customer_type,
  day_of_week,
  ROUND(AVG(ride_length_seconds), 2)               AS avg_ride_length
FROM all_trips_clean
GROUP BY member_casual, day_of_week
ORDER BY customer_type, day_of_week;
