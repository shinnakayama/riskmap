#standardSQL

-- upload port_risk.csv to a directory `gfwanalysis.qualtrics_survey.port_risk`

--SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (TIMESTAMP("2012-01-01"));
--
  --SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (TIMESTAMP("2019-12-31"));
--
  -- port stops less this value (in hours) excluded and voyages merged
CREATE TEMP FUNCTION min_port_stop() AS (CAST(0 AS INT64));
--
  -- final voyages with durations of less this value (in hours) are excluded
CREATE TEMP FUNCTION min_trip_duration() AS (CAST(2 AS INT64));
--
--
------------------------------------------------------------
-- Raw data from the voyage table, removing some known noise
------------------------------------------------------------
WITH

trip_ids AS (
   SELECT * FROM (
      SELECT
         ssvid,
         vessel_ids,
         CAST(IF(trip_start < TIMESTAMP("1900-01-01"), NULL, trip_start) AS TIMESTAMP) AS trip_start,
         CAST(IF(trip_end > TIMESTAMP("2099-12-31"), NULL, trip_end) AS TIMESTAMP) AS trip_end,
         trip_start_anchorage_id,
         trip_end_anchorage_id
   FROM (SELECT * FROM `world-fishing-827.gfw_research.voyages_no_overlapping_short_seg_v20200819`
   WHERE trip_start <= maximum()
      AND trip_end >= minimum()
      AND trip_start_anchorage_id != "10000001"
      AND trip_end_anchorage_id != "10000001")
   )
),

------------------------------------------------
-- anchorage ids that represent the Panama Canal
------------------------------------------------
panama_canal_ids AS (
   SELECT s2id AS anchorage_id
   FROM `world-fishing-827.anchorages.named_anchorages_v20201104`
   WHERE sublabel="PANAMA CANAL"
),
-----------------------------------------------------
-- Add ISO3 flag code to trip start and end anchorage
-----------------------------------------------------
add_trip_start_end_iso3 AS (
   SELECT
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      b.iso3 AS start_anchorage_iso3,
      trip_end_anchorage_id,
      c.iso3 AS end_anchorage_iso3,
      TIMESTAMP_DIFF(trip_end, trip_start, SECOND) / 3600 AS trip_duration_hr
   FROM trip_ids a
   LEFT JOIN `world-fishing-827.anchorages.named_anchorages_v20201104` b
   ON a.trip_start_anchorage_id = b.s2id
   LEFT JOIN `world-fishing-827.anchorages.named_anchorages_v20201104` c
   ON a.trip_end_anchorage_id = c.s2id
   GROUP BY 1,2,3,4,5,6,7,8
),
-------------------------------------------------------------------
-- Mark whether start anchorage or end anchorage is in Panama canal
-- This is to remove trips within Panama Canal
-------------------------------------------------------------------
is_end_port_pan AS (
   SELECT
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id ,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_iso3,
      IF (trip_end_anchorage_id IN (SELECT anchorage_id FROM panama_canal_ids ),
         TRUE, FALSE ) current_end_is_panama,
      IF (trip_start_anchorage_id IN (SELECT anchorage_id FROM panama_canal_ids ),
         TRUE, FALSE ) current_start_is_panama,
   FROM add_trip_start_end_iso3
),

------------------------------------------------
-- Add information about
-- whether previous and next ports are in Panama
------------------------------------------------
add_prev_next_port AS (
   SELECT
      *,
      IFNULL (
         LAG (trip_start, 1) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start ASC ),
         TIMESTAMP ("2000-01-01") ) AS prev_trip_start,
      IFNULL (
         LEAD (trip_end, 1) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start ASC ),
         TIMESTAMP ("2100-01-01") ) AS next_trip_end,
      LAG (current_end_is_panama, 1) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start ASC ) AS prev_end_is_panama,
      LEAD (current_end_is_panama, 1) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start ASC ) AS next_end_is_panama,
   FROM is_end_port_pan
),

---------------------------------------------------------------------------------
-- Mark the start and end of the block. The start of the block is the anchorage
-- just before Panama canal, and the end of the block is the anchorage just after
-- Panama canal (all consecutive trips within Panama canal will be ignored later).
-- If there is no Panama canal involved in a trip, the start/end of the block are
-- the trip start/end of that trip.
---------------------------------------------------------------------------------
block_start_end AS (
   SELECT
      *,
      IF (prev_end_is_panama, NULL, trip_start) AS block_start,
      IF (current_end_is_panama, NULL, trip_end) AS block_end
      -- IF (current_start_is_panama AND prev_end_is_panama, NULL, trip_start) AS block_start,
      -- IF (current_end_is_panama AND next_start_is_panama, NULL, trip_end) AS block_end
   FROM add_prev_next_port
),

-------------------------------------------
-- Find the closest non-Panama ports
-- by looking ahead and back of the records
-------------------------------------------
look_back_and_ahead AS (
   SELECT
      * EXCEPT(block_start, block_end),
      LAST_VALUE (block_start IGNORE NULLS) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS block_start,
      FIRST_VALUE (block_end IGNORE NULLS) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start
         ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS block_end
   FROM block_start_end
),

-------------------------------------------------------------------
-- Within a block, all trips will have the same information
-- about their block (start / end of the block, anchorage start/end
-------------------------------------------------------------------
blocks_to_be_collapsed_down AS (
   SELECT
      ssvid,
      block_start,
      block_end,
      FIRST_VALUE (trip_start_anchorage_id) OVER (
         PARTITION BY block_start, block_end
         ORDER BY trip_start ASC) AS trip_start_anchorage_id,
      FIRST_VALUE (start_anchorage_iso3) OVER (
         PARTITION BY block_start, block_end
         ORDER BY trip_start ASC) AS start_anchorage_iso3,
      FIRST_VALUE (trip_end_anchorage_id) OVER (
         PARTITION BY block_start, block_end
         ORDER BY trip_end DESC) AS trip_end_anchorage_id,
      FIRST_VALUE (end_anchorage_iso3) OVER (
         PARTITION BY block_start, block_end
         ORDER BY trip_end DESC) AS end_anchorage_iso3,
   FROM look_back_and_ahead
),

---------------------------------------------------------------------
-- Blocks get collapsed down to one row, which means a block of trips
-- becomes a complete trip
---------------------------------------------------------------------
updated_pan_voyages AS (
   SELECT
      ssvid,
      block_start AS trip_start,
      block_end AS trip_end,
      trip_start_anchorage_id,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_iso3
   FROM blocks_to_be_collapsed_down
   GROUP BY 1,2,3,4,5,6,7
),

----------------------------------------------------------------------
-- Identify port stops that are too short, which indicates a vessel
-- to consider its trip as stopping there
-- First of all, add port stop duration (at the end of current voyage)
----------------------------------------------------------------------
add_port_stop_duration AS (
   SELECT
      * EXCEPT (next_voyage_start),
      TIMESTAMP_DIFF(next_voyage_start, trip_end, SECOND) / 3600 AS port_stop_duration_hr
   FROM (
      SELECT
         *,
         LEAD(trip_start, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_start
      FROM updated_pan_voyages
   )
),

---------------------------------------------------------
-- Determine if the current, previous, or next port stops
-- are *too* short, with a threshold
---------------------------------------------------------
is_port_too_short AS (
   SELECT
      *,
      LAG (current_port_too_short, 1) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start ASC) AS prev_port_too_short,
      LEAD (current_port_too_short, 1) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start ASC) AS next_port_too_short,
      FROM (
      SELECT
         *,
         IF (port_stop_duration_hr < min_port_stop() AND port_stop_duration_hr IS NOT NULL,
         TRUE, FALSE ) AS current_port_too_short
   FROM add_port_stop_duration)
),

---------------------------------------------------------------------------------------
-- Mark the start and end of the "voyage". Short port visits are to be combined
-- with the closest prev/next "long" port visit to ignore just "pass-by" trips to ports
---------------------------------------------------------------------------------------
voyage_start_end AS (
   SELECT
      * EXCEPT (prev_port_too_short, current_port_too_short),
      IF (prev_port_too_short, NULL, trip_start) AS voyage_start,
      IF (current_port_too_short, NULL, trip_end) AS voyage_end
   FROM is_port_too_short
),

----------------------------------------------------------------
  -- Find the closest not-too-short port visits in prev/next ports
-- by looking ahead and back of the records
----------------------------------------------------------------
look_back_and_ahead_for_voyage AS (
   SELECT
      * EXCEPT(voyage_start, voyage_end),
      LAST_VALUE (voyage_start IGNORE NULLS) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS voyage_start,
      FIRST_VALUE (voyage_end IGNORE NULLS) OVER (
         PARTITION BY ssvid
         ORDER BY trip_start
         ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS voyage_end
   FROM voyage_start_end
   ),

--------------------------------------------------------------------------
-- Within a "voyage", all trips that are to be grouped (due to short stops)
-- will contain the same information about its voyages start/end anchorage
---------------------------------------------------------------------------
voyages_to_be_collapsed_down AS (
   SELECT
      ssvid,
      voyage_start,
      voyage_end,
      FIRST_VALUE (trip_start_anchorage_id) OVER (
         PARTITION BY voyage_start, voyage_end
         ORDER BY trip_start ASC) AS trip_start_anchorage_id,
      FIRST_VALUE (start_anchorage_iso3) OVER (
         PARTITION BY voyage_start, voyage_end
         ORDER BY trip_start ASC) AS start_anchorage_iso3,
      FIRST_VALUE (trip_end_anchorage_id) OVER (
         PARTITION BY voyage_start, voyage_end
         ORDER BY trip_start DESC) AS trip_end_anchorage_id,
      FIRST_VALUE (end_anchorage_iso3) OVER (
         PARTITION BY voyage_start, voyage_end
         ORDER BY trip_start DESC) AS end_anchorage_iso3,
      FIRST_VALUE (port_stop_duration_hr) OVER (
         PARTITION BY voyage_start, voyage_end
         ORDER BY trip_start DESC) AS port_stop_duration_hr,
   FROM look_back_and_ahead_for_voyage
),

----------------------------------------------------------------------
-- Blocks get collapsed down to one row, which means a block of voyage
-- becomes a complete voyage (combining all too-short port visits
----------------------------------------------------------------------
updated_voyages AS (
   SELECT
      ssvid,
      voyage_start AS trip_start,
      voyage_end AS trip_end,
      trip_start_anchorage_id,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_iso3,
      port_stop_duration_hr
      FROM voyages_to_be_collapsed_down
   GROUP BY 1,2,3,4,5,6,7,8
),

-----------------------------------------------------------
  -- Add information about trip_start and trip_end anchorages
-----------------------------------------------------------
trip_start_end_label AS (
   SELECT
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      b.lat AS start_anchorage_lat,
      b.lon AS start_anchorage_lon,
      b.label AS start_anchorage_label,
      b.iso3 AS start_anchorage_iso3,
      trip_end_anchorage_id,
      c.lat AS end_anchorage_lat,
      c.lon AS end_anchorage_lon,
      c.label AS end_anchorage_label,
      c.iso3 AS end_anchorage_iso3,
      TIMESTAMP_DIFF(trip_end, trip_start, SECOND) / 3600 AS trip_duration_hr,
      port_stop_duration_hr
   FROM updated_voyages AS a
   LEFT JOIN `world-fishing-827.anchorages.named_anchorages_v20201104`  AS b
   ON a.trip_start_anchorage_id = b.s2id
   LEFT JOIN `world-fishing-827.anchorages.named_anchorages_v20201104` AS c
   ON a.trip_end_anchorage_id = c.s2id
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
),

------------------------------------------------------------
  -- Filter all trips to 2 hour duration or no start, or no end
------------------------------------------------------------
generate_final_trips AS (
   SELECT
      *,
      IF(trip_start_anchorage_id = 'NO_PREVIOUS_DATA',
      concat(ssvid,"-",
      format("%012x",
      timestamp_diff(TIMESTAMP('0001-02-03 00:00:00'),
      timestamp("1970-01-01"),
      MILLISECOND))),
      concat(ssvid, "-",
      format("%012x",
      timestamp_diff(trip_start,
      timestamp("1970-01-01"),
      MILLISECOND))
      )) as gfw_trip_id
   FROM trip_start_end_label
   WHERE (
      (trip_end >= minimum() OR trip_end IS NULL) )
      AND (trip_end_anchorage_id = "ACTIVE_VOYAGE"
      OR trip_duration_hr > min_trip_duration()
      OR trip_start_anchorage_id = "NO_PREVIOUS_DATA")
      AND (trip_start <= maximum()
      OR trip_start IS NULL
   )
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),


-- add start lon/lat
trip_start_coords AS (
  SELECT
    * EXCEPT(s2id, lon, lat),
    ST_GEOGPOINT(lon, lat) AS trip_start_anchorage_coords
  FROM generate_final_trips AS a
  LEFT JOIN (select s2id, lon, lat FROM `world-fishing-827.anchorages.named_anchorages_v20201104`) AS b
  ON a.trip_start_anchorage_id = b.s2id
),

-- add end lon/lat
trip_end_coords AS (
  SELECT
    * EXCEPT(s2id, lon, lat),
    ST_GEOGPOINT(lon, lat) AS trip_end_anchorage_coords,
  FROM trip_start_coords AS a
  LEFT JOIN (select s2id, lon, lat FROM `world-fishing-827.anchorages.named_anchorages_v20201104`) AS b
  ON a.trip_end_anchorage_id = b.s2id
),


-- GFW anchorage table
GFW_anchorage AS (
   SELECT
      label,
      s2id,
      lat AS anchorage_lat,
      lon AS anchorage_lon,
      ST_GEOGPOINT(lon, lat) AS anchorage_coords
   FROM `world-fishing-827.anchorages.named_anchorages_v20201104`
),


COS_port AS (
   SELECT
      not_associated,
      iuu_low, iuu_med, iuu_high,
      la_low, la_med, la_high,
      lat AS port_lat,
      lon AS port_lon,
      ST_GEOGPOINT(lon, lat) AS port_coords,
      port_id
   FROM `gfwanalysis.qualtrics_survey.port_risk`
),


-- match
GFW_anchorage_with_port AS (
   SELECT
      s2id,
      label,
      ARRAY_AGG(port_id ORDER BY ST_DISTANCE(anchorage_coords, port_coords) LIMIT 1) [ORDINAL(1)] AS port_id,
   FROM GFW_anchorage
   JOIN COS_port
   ON ST_DWITHIN(anchorage_coords, port_coords, 3000) -- search within 3 km
   GROUP BY s2id, label
),


-- add GFW anchorage coords
GFW_anchorage_with_port2 AS (
   SELECT * EXCEPT(x) FROM GFW_anchorage_with_port AS a
   LEFT JOIN (SELECT s2id as x, anchorage_lat, anchorage_lon, anchorage_coords FROM GFW_anchorage) AS b
   ON a.s2id = b.x
),


-- add COS port coords
GFW_anchorage_with_port3 AS (
   SELECT * EXCEPT(x) FROM GFW_anchorage_with_port2 AS a
   LEFT JOIN (SELECT port_id AS x, port_coords FROM COS_port) AS b
   ON a.port_id = b.x
),


-- add distance between anchorage and port
GFW_anchorage_with_port4 AS (
   SELECT
      *,
      ST_DISTANCE(anchorage_coords, port_coords)/1000 AS distance_km
   FROM GFW_anchorage_with_port3
),


-- rank distance between GFW anchorage and COS port within each label
rank_distance AS (
   SELECT
      s2id,
      label,
      port_id,
      distance_km,
      ROW_NUMBER() OVER(PARTITION BY port_id ORDER BY distance_km ASC) AS rank
   FROM GFW_anchorage_with_port4
),


-- get the shortest within each label
port_summary AS (
   SELECT * EXCEPT(rank)
   FROM rank_distance
   WHERE rank = 1
),

-------------------------
-- match with trip data
-- add start anchorage label
trip_start_anchor_label AS (
   SELECT * EXCEPT(s2id) FROM trip_end_coords AS a
   LEFT JOIN (SELECT label AS trip_start_anchorage_label, s2id FROM GFW_anchorage) AS b
   ON a.trip_start_anchorage_id = b.s2id
),
-- add start port
trip_start_port AS (
   SELECT * EXCEPT(x) FROM trip_start_anchor_label AS a
   LEFT JOIN (SELECT port_id AS trip_start_port_id, label AS x FROM port_summary) AS b
   ON a.trip_start_anchorage_label = b.x
),


-- add end anchorage label
trip_end_anchor_label AS (
   SELECT * EXCEPT(s2id) FROM trip_start_port AS a
   LEFT JOIN (SELECT label AS trip_end_anchorage_label, s2id FROM GFW_anchorage) AS b
   ON a.trip_end_anchorage_id = b.s2id
),
-- add end port
trip_end_port AS (
   SELECT * EXCEPT(x) FROM trip_end_anchor_label AS a
   LEFT JOIN (SELECT port_id AS trip_end_port_id, label AS x FROM port_summary) AS b
   ON a.trip_end_anchorage_label = b.x
),


-- add port risk
risk AS (
   SELECT
      port,
      country,
      port_id AS x,
      not_associated,
      iuu_low, iuu_med, iuu_high,
      la_low, la_med, la_high
   FROM `gfwanalysis.qualtrics_survey.port_risk`
),
trip_with_start_port_risk AS (
   SELECT * EXCEPT(x) FROM trip_end_port AS a
   LEFT JOIN (
      SELECT
         x,
         port AS from_port,
         country AS from_country,
         not_associated AS from_iuu_no,
         iuu_low AS from_iuu_low,
         iuu_med AS from_iuu_med,
         iuu_high AS from_iuu_high,
         not_associated AS from_la_no,
         la_low AS from_la_low,
         la_med AS from_la_med,
         la_high AS from_la_high
      FROM risk
   ) AS b
   ON a.trip_start_port_id = b.x
),
trip_with_end_port_risk AS (
   SELECT * EXCEPT(x) FROM trip_with_start_port_risk AS a
   LEFT JOIN (
      SELECT
         x,
         port AS to_port,
         country AS to_country,
         not_associated AS to_iuu_no,
         iuu_low AS to_iuu_low,
         iuu_med AS to_iuu_med,
         iuu_high AS to_iuu_high,
         not_associated AS to_la_no,
         la_low AS to_la_low,
         la_med AS to_la_med,
         la_high AS to_la_high
      FROM risk
   ) AS b
   ON a.trip_end_port_id = b.x
),


encounters AS (
   SELECT
      event_id,
      vessel_id,
      event_start,
      event_end,
      lat_mean,
      lon_mean,
      JSON_EXTRACT(event_info,
      "$.median_distance_km") AS median_distance_km,
      JSON_EXTRACT(event_info,
      "$.median_speed_knots") AS median_speed_knots,
      SPLIT(event_id, ".")[ORDINAL(1)] AS event,
      CAST (event_start AS DATE) event_date,
      EXTRACT(YEAR FROM event_start) AS year
   FROM
      `world-fishing-827.pipe_production_v20190502.published_events_encounters`
   WHERE
      event_end <= TIMESTAMP('2019-12-31')
      AND lat_mean < 90
      AND lat_mean > -90
      AND lon_mean < 180
      AND lon_mean > -180
),


-- grab daily information on ssvid corresponding to vessel_id
ssvid_map AS (
   SELECT vessel_id, ssvid, day
   FROM `world-fishing-827.pipe_production_v20190502.segment_vessel_daily_*`
),


-- Join the encounters data with the ssvid data on the same vessel_id and event day to ensure correct SSVID
encounter_ssvid AS (
   SELECT * EXCEPT(vessel_id)
   FROM (SELECT * FROM encounters) a
   JOIN (SELECT * FROM ssvid_map) b
   ON a.vessel_id = b.vessel_id
      AND a.event_date = b.day
),


-- create curated carrier list
carrier_vessels AS (
   SELECT
      identity.ssvid AS carrier_ssvid,
      identity.imo AS carrier_imo_ais,
      identity.n_shipname AS carrier_shipname_ais,
      identity.n_callsign AS carrier_callsign_ais,
      first_timestamp AS carrier_first_timestamp,
      last_timestamp AS carrier_last_timestamp
   FROM
      `world-fishing-827.vessel_database.all_vessels_v20200901`,
      UNNEST(registry),
      UNNEST(activity)
   WHERE is_carrier
      AND confidence = 3
      AND identity.ssvid NOT IN ('111111111','0','888888888','416202700')
      AND first_timestamp <= timestamp('2019-12-31')
   GROUP BY 1,2,3,4,5,6
),


-- Identify encounters with carriers
encounters_carriers AS(
   SELECT *
   FROM (SELECT * FROM encounter_ssvid) a
   JOIN (SELECT * FROM carrier_vessels) b
   ON a.ssvid = SAFE_CAST(b.carrier_ssvid AS STRING)
      AND a.event_start BETWEEN b.carrier_first_timestamp AND b.carrier_last_timestamp
      AND a.event_end BETWEEN b.carrier_first_timestamp AND b.carrier_last_timestamp
),


-- Join vessel the carrier encountered
all_encounters as (
   SELECT
      event_id,
      carrier_ssvid,
      neighbor_ssvid,
      event_start,
      event_end,
      lat_mean,
      lon_mean,
      median_distance_km,
      median_speed_knots,
      (TIMESTAMP_DIFF(event_end,event_start,minute)/60) event_duration_hr,
      a.event AS event,
      event_date
   FROM
      (SELECT * FROM encounters_carriers) a
   JOIN (SELECT ssvid AS neighbor_ssvid, event FROM encounter_ssvid) b
   ON a.event = b.event
   WHERE carrier_ssvid != neighbor_ssvid
   GROUP BY
   1,2,3,4,5,6,7,8,9,10,11,12
),


-- add vessel info to the carriers
carrier_vessel_info as(
   SELECT *
   FROM (SELECT * FROM all_encounters) a
   LEFT JOIN (
      SELECT
      ssvid,
      ais_identity.n_shipname_mostcommon.value as carrier_shipname,
      ais_identity.n_imo_mostcommon.value as carrier_imo,
      ais_identity.n_callsign_mostcommon.value as carrier_callsign,
      IF(best.best_flag = 'UNK', ais_identity.flag_mmsi, best.best_flag) as carrier_flag,
      best.best_vessel_class as carrier_label,
      activity.first_timestamp as first_timestamp,
      activity.last_timestamp as last_timestamp,
      activity.frac_spoofing as carrier_spoofing
   FROM
      `world-fishing-827.gfw_research.vi_ssvid_v20200801`
   ) b
   ON SAFE_CAST(a.carrier_ssvid as int64) = SAFE_CAST(b.ssvid as int64)
      AND a.event_start >= b.first_timestamp
      AND a.event_end <= b.last_timestamp
),


-- Add the vessel info to the neighbor vessel
neighbor_vessel_info as(
   SELECT * FROM (SELECT * FROM carrier_vessel_info) a
   LEFT JOIN (
      SELECT
         ssvid,
         ais_identity.n_shipname_mostcommon.value  as neighbor_shipname,
         ais_identity.n_imo_mostcommon.value as neighbor_imo,
         ais_identity.n_callsign_mostcommon.value as neighbor_callsign,
         IF(best.best_flag = 'UNK', ais_identity.flag_mmsi, best.best_flag) as neighbor_flag,
         best.best_vessel_class as neighbor_label,
         activity.first_timestamp as neighbor_first_timestamp,
         activity.last_timestamp as neighbor_last_timestamp,
         activity.frac_spoofing as neighbor_spoofing
      FROM
         `world-fishing-827.gfw_research.vi_ssvid_v20200801`
   ) b
   ON SAFE_CAST(a.neighbor_ssvid as int64) = SAFE_CAST(b.ssvid as int64)
      AND a.event_start >= b.neighbor_first_timestamp
      AND a.event_end <= b.neighbor_last_timestamp
),


--Clean up the data to the columns we are interested in, limiting the data to only those vessels that rarely spoof,
--and group data to remove any possible duplications generated in the process of creating data
encounter_clean AS (
   SELECT
      event,
      event_id,
      carrier_ssvid,
      carrier_shipname,
      carrier_imo,
      carrier_callsign,
      carrier_flag,
      carrier_label,
      neighbor_ssvid,
      neighbor_shipname,
      neighbor_imo,
      neighbor_callsign,
      neighbor_flag,
      neighbor_label,
      event_start,
      event_end,
      lat_mean,
      lon_mean,
      median_distance_km,
      median_speed_knots,
      event_duration_hr
   FROM
      neighbor_vessel_info
   WHERE carrier_spoofing < 0.05
      AND neighbor_spoofing < 0.05
   GROUP BY
      event,
      event_id,
      carrier_ssvid,
      carrier_shipname,
      carrier_imo,
      carrier_callsign,
      carrier_flag,
      carrier_label,
      neighbor_ssvid,
      neighbor_shipname,
      neighbor_imo,
      neighbor_callsign,
      neighbor_flag,
      neighbor_label,
      event_start,
      event_end,
      lat_mean,
      lon_mean,
      median_distance_km,
      median_speed_knots,
      event_duration_hr
),


-- filter encounter > 10 km from the latest anchorage points
anchorages AS (
   SELECT
      *,
      ST_GEOGPOINT(lon, lat) AS anchorage_point
   FROM `world-fishing-827.anchorages.named_anchorages_v20201104`
),


encounter_near_anchorage AS (
   SELECT
      event_id
   FROM (
      SELECT *, ST_GEOGPOINT(lon_mean, lat_mean) AS encounter_point
      FROM encounter_clean
   ) AS a
   CROSS JOIN (SELECT * FROM anchorages) AS b
   WHERE ST_DISTANCE(encounter_point, anchorage_point) < 10000
   group by event_id
),


encounter_not_in_port AS (
   SELECT * FROM encounter_clean
   WHERE event_id NOT IN (
      SELECT event_id FROM encounter_near_anchorage
   )
),


-- find trips with encounters
trip_with_encounter AS (
  SELECT * EXCEPT(ssvid) FROM trip_with_end_port_risk AS a
  LEFT JOIN (SELECT * FROM encounter_not_in_port) AS b
  ON a.ssvid = b.carrier_ssvid
  WHERE trip_start < event_start
  AND trip_end > event_end
),


-- add flag state
encounter_carrier_flag AS (
   SELECT * EXCEPT (iso3) FROM trip_with_encounter AS a
   LEFT JOIN (SELECT iso3, country_name AS carrier_flag_state FROM `world-fishing-827.gfw_research.country_codes`) AS b
   ON a.carrier_flag = b.iso3
),
encounter_neighbor_flag AS (
   SELECT * EXCEPT(iso3) FROM encounter_carrier_flag AS a
   LEFT JOIN (SELECT iso3, country_name AS neighbor_flag_state FROM `world-fishing-827.gfw_research.country_codes`) AS b
   ON a.neighbor_flag = b.iso3
),


-- remove cases where trip_start < first_timestamp
transshipment AS (
   SELECT * EXCEPT(ssvid) FROM encounter_neighbor_flag AS a
   LEFT JOIN (
     SELECT
       ssvid,
       activity.first_timestamp,
       on_fishing_list_best AS is_fishing
     FROM `world-fishing-827.gfw_research.vi_ssvid_v20200801`
   ) AS b
   ON a.neighbor_ssvid = b.ssvid
   WHERE trip_start >= first_timestamp
      AND is_fishing IS TRUE
),


transshipment_clean AS (
   SELECT
      gfw_trip_id,
      event_start,
      event_end,
      trip_start, trip_end, trip_duration_hr,
      from_iuu_no, from_iuu_low, from_iuu_med, from_iuu_high,
      to_iuu_no, to_iuu_low, to_iuu_med, to_iuu_high,
      from_la_no, from_la_low, from_la_med, from_la_high,
      to_la_no, to_la_low, to_la_med, to_la_high,
      from_country, to_country,
      from_port, to_port,
      trip_start_port_id, trip_end_port_id,
      carrier_flag, neighbor_flag,
      carrier_flag_state, neighbor_flag_state,
      carrier_label, neighbor_label,
      carrier_ssvid, neighbor_ssvid,
      lon_mean, lat_mean
   FROM transshipment
   GROUP BY
      gfw_trip_id,
      event_start,
      event_end,
      trip_start, trip_end, trip_duration_hr,
      from_iuu_no, from_iuu_low, from_iuu_med, from_iuu_high,
      to_iuu_no, to_iuu_low, to_iuu_med, to_iuu_high,
      from_la_no, from_la_low, from_la_med, from_la_high,
      to_la_no, to_la_low, to_la_med, to_la_high,
      from_country, to_country,
      from_port, to_port,
      trip_start_port_id, trip_end_port_id,
      carrier_flag, neighbor_flag,
      carrier_flag_state, neighbor_flag_state,
      carrier_label, neighbor_label,
      carrier_ssvid, neighbor_ssvid,
      lon_mean, lat_mean
),


------------------------------------------
-- add feature classes
transshipment_feature AS (
   SELECT
      gfw_trip_id,
      trip_start,
      trip_end,
      from_country, to_country,
      from_port, to_port,
      trip_start_port_id, trip_end_port_id,
      lon_mean, lat_mean,
      event_start,
      event_end,
      from_iuu_no, from_iuu_low, from_iuu_med, from_iuu_high,
      to_iuu_no, to_iuu_low, to_iuu_med, to_iuu_high,
      from_la_no, from_la_low, from_la_med, from_la_high,
      to_la_no, to_la_low, to_la_med, to_la_high,
      carrier_ssvid, neighbor_ssvid,
      carrier_flag, neighbor_flag,

      CASE
         WHEN carrier_flag IN ('ATG','BRB','CYM','LBR','VCT','VUT') THEN 'group1'
         WHEN carrier_flag IN ('BHS','BHR','BLZ','BOL','BRN','KHM','CYP','GNQ','GAB','GEO','HND','KIR','MDG','MLT',
            'MHL','PAN','PRT','KNA','WSM','SLE','LKA','TON','TZA') THEN 'group2'
         WHEN carrier_flag IN ('ALB','DZA','AGO','AIA','ARG','AUS','AZE','BGD','BEL','BMU','BRA','BGR','CPV',
            'CMR','CAN','CHL','HKG','TWN','COL','COD','CRI','HRV','CUB','DNK','DJI','ECU',
            'EGY','ERI','EST','ETH','FJI','FIN','FRA','GMB','DEU','GHA','GRC','GRL','GRD',
            'GTM','GUY','ISL','IND','IDN','IRN','IRQ','IRL','ISR','ITA','JPN','JOR','KAZ',
            'KEN','PRK','KOR','KWT','LAO','LVA','LBN','LBY','LTU','LUX','MYS','MDV','MRT',
            'MUS','MEX','MNE','MAR','MOZ','MMR','NAM','NLD','NZL','NGA','NOR','OMN','PAK',
            'PNG','PRY','PER','PHL','POL','QAT','RUS','SAU','SEN','SYC','SGP','SVN','ZAF',
            'ESP','SDN','SUR','SWE','CHE','SYR','THA','TTO','TUN','TUR','TKM','UKR','ARE',
            'GBR','USA','URY','VEN','VNM','YEM') THEN 'group3'
         WHEN carrier_flag = 'CHN' THEN 'china'
         WHEN carrier_flag IS NULL THEN NULL
         ELSE 'other'
      END AS carrier_flag_group,

      CASE
         WHEN neighbor_flag IN ('ATG','BRB','CYM','LBR','VCT','VUT') THEN 'group1'
         WHEN neighbor_flag IN ('BHS','BHR','BLZ','BOL','BRN','KHM','CYP','GNQ','GAB','GEO','HND','KIR','MDG','MLT',
            'MHL','PAN','PRT','KNA','WSM','SLE','LKA','TON','TZA') THEN 'group2'
         WHEN neighbor_flag IN ('ALB','DZA','AGO','AIA','ARG','AUS','AZE','BGD','BEL','BMU','BRA','BGR','CPV',
            'CMR','CAN','CHL','HKG','TWN','COL','COD','CRI','HRV','CUB','DNK','DJI','ECU',
            'EGY','ERI','EST','ETH','FJI','FIN','FRA','GMB','DEU','GHA','GRC','GRL','GRD',
            'GTM','GUY','ISL','IND','IDN','IRN','IRQ','IRL','ISR','ITA','JPN','JOR','KAZ',
            'KEN','PRK','KOR','KWT','LAO','LVA','LBN','LBY','LTU','LUX','MYS','MDV','MRT',
            'MUS','MEX','MNE','MAR','MOZ','MMR','NAM','NLD','NZL','NGA','NOR','OMN','PAK',
            'PNG','PRY','PER','PHL','POL','QAT','RUS','SAU','SEN','SYC','SGP','SVN','ZAF',
            'ESP','SDN','SUR','SWE','CHE','SYR','THA','TTO','TUN','TUR','TKM','UKR','ARE',
            'GBR','USA','URY','VEN','VNM','YEM') THEN 'group3'
         WHEN neighbor_flag = 'CHN' THEN 'china'
         WHEN neighbor_flag IS NULL THEN NULL
         ELSE 'other'
      END AS neighbor_flag_group,

      COALESCE (
         CASE WHEN trip_duration_hr < 24*30*1 THEN 'less_than_1m' ELSE NULL END,
         CASE WHEN trip_duration_hr < 24*30*3 THEN '1_3m' ELSE NULL END,
         CASE WHEN trip_duration_hr < 24*30*6 THEN '3_6m' ELSE NULL END,
         CASE WHEN trip_duration_hr < 24*30*12 THEN '6_12m' ELSE NULL END,
         CASE WHEN trip_duration_hr >= 24*30*12 THEN '12m_and_more' ELSE NULL END
      ) AS time_at_sea,

      CASE
         WHEN neighbor_label = 'trawlers' THEN 'trawlers'
         WHEN neighbor_label = 'trollers' THEN 'trollers'
         WHEN neighbor_label = 'driftnets' THEN 'driftnets'
         WHEN neighbor_label IN ('purse_seines', 'tuna_purse_seines', 'other_purse_seines') THEN 'purse_seine'
         WHEN neighbor_label = 'set_gillnets' THEN 'set_gillnet'
         WHEN neighbor_label = 'squid_jigger' THEN 'squid_jigger'
         WHEN neighbor_label = 'pole_and_line' THEN 'pole_and_line'
         WHEN neighbor_label = 'set_longlines' THEN 'set_longline'
         WHEN neighbor_label = 'pots_and_traps' THEN 'pots_and_traps'
         WHEN neighbor_label = 'drifting_longlines' THEN 'drifting_longline'
         ELSE NULL
      END AS neighbor_vessel_class

   FROM transshipment_clean
)


SELECT *
FROM transshipment_feature
