#standardSQL

WITH

-- load data
trip_with_risk_class AS (
   SELECT *
   FROM `gfwanalysis.GFW_trips.fishing_la_departure`
),


-- This subquery identifies good segments
good_segments AS (
   SELECT seg_id
   FROM `world-fishing-827.gfw_research.pipe_v20200805_segs`
   WHERE good_seg
      AND positions > 10
      AND NOT overlapping_and_short
),


-- fishing with good segments
fishing AS (
   SELECT
      ssvid AS x,
      timestamp,
      lat,
      lon,
      IF(nnet_score2 > 0.5, hours, 0) as fishing_hours
   FROM
      `world-fishing-827.gfw_research.pipe_v20200805_fishing`
   WHERE
      seg_id IN (SELECT seg_id FROM good_segments)
),


-- merge
fishing_with_trip AS (
   SELECT
      * EXCEPT(x) FROM fishing AS a
   LEFT JOIN (
      SELECT *
      FROM trip_with_risk_class
   ) AS b
   ON a.x = CAST(b.ssvid AS STRING)
   WHERE trip_start <= timestamp
      AND trip_end >= timestamp
),


-- round coordinates
fishing_coord AS (
   SELECT
      ROUND(lat) AS lat_bin,
      ROUND(lon) AS lon_bin,
      fishing_hours,
      risk_class
   FROM fishing_with_trip
),


-- by risk_class
fishing_binned AS (
   SELECT
      lat_bin,
      lon_bin,
      SUM(fishing_hours) AS fishing_hours,
      risk_class
   FROM fishing_coord
   GROUP BY lat_bin, lon_bin, risk_class
)


SELECT *
FROM fishing_binned
