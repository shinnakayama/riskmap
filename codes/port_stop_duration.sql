WITH

event_id AS (
  SELECT * EXCEPT(vessel_id),
  vessel_id AS x,
  CONCAT(vessel_id, start_timestamp) AS visit_start_id,
  CONCAT(vessel_id, end_timestamp) AS visit_end_id,
  CONCAT(vessel_id, start_timestamp, end_timestamp) as visit_id
  FROM `world-fishing-827.pipe_production_v20190502.port_visits_*`
  WHERE _TABLE_SUFFIX BETWEEN '20120101' AND '20191231'
),

port_event AS (
  SELECT * EXCEPT(events)
  FROM event_id,
  UNNEST(events)
),

port_event_clean AS (
   SELECT
      start_timestamp,
      end_timestamp,
      EXTRACT(DATE FROM start_timestamp) AS date,
      EXTRACT(YEAR FROM start_timestamp) AS year,
      timestamp,
      anchorage_id,
      event_type,
      visit_id,
      x AS vessel_id
   FROM port_event
),

-- take only very plausible sequences
-- (entry/end) -> begin -> (gap) -> end -> (begin/exit)
port_event_weird_sequence AS (
   SELECT
      *,
      CASE
         WHEN LAG(event_type, 0) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_BEGIN'
            AND LAG(event_type, 1) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_GAP'
         THEN 'weird_begin'
         WHEN LEAD(event_type, 0) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_END'
            AND LEAD(event_type, 1) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_GAP'
         THEN 'weird_end'
      END flag
   FROM port_event_clean
   ORDER BY visit_id, timestamp
),

-- keep only 'PORT_STOP_BEGIN' and 'PORT_STOP_END'
port_event_clean2 AS (
   SELECT *
   FROM port_event_weird_sequence
   WHERE event_type IN ('PORT_STOP_BEGIN', 'PORT_STOP_END')
   ORDER BY visit_id, timestamp
),

-- find the other piece of the bracket of weird stop begin/end
port_event_weird_sequence2 AS (
   SELECT
      *,
      CASE
         WHEN LAG(flag,1) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'weird_begin'
            AND LAG(event_type,0) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_END'
         THEN 'weird_end'
         WHEN LEAD(flag,1) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'weird_end'
            AND LEAD(event_type,0) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_BEGIN'
         THEN 'weird_begin'
      END flag2
   FROM port_event_clean2
   ORDER BY visit_id, timestamp
),

-- remove weird sequences
port_event_clean3 AS (
   SELECT * EXCEPT (flag, flag2)
   FROM port_event_weird_sequence2
   WHERE flag IS NULL and flag2 IS NULL
   ORDER BY visit_id, timestamp
),

-- get gap (min) between consecutive port stop events
-- when it is at the same anchorage during the same port visit
port_event_gap AS (
   SELECT
      *,
      CASE
         WHEN LAG(event_type, 0) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_BEGIN'
            AND LAG(event_type, 1) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_END'
            AND LAG(visit_id, 0) OVER(PARTITION BY visit_id ORDER BY timestamp) = LAG(visit_id, 1) OVER(PARTITION BY visit_id ORDER BY timestamp)
            AND LAG(anchorage_id, 0) OVER(PARTITION BY visit_id ORDER BY timestamp) = LAG(anchorage_id, 1) OVER(PARTITION BY visit_id ORDER BY timestamp)
         THEN TIMESTAMP_DIFF(LAG(timestamp,0) OVER(PARTITION BY visit_id ORDER BY timestamp), LAG(timestamp,1) OVER(PARTITION BY visit_id ORDER BY timestamp), MINUTE)
      END gap_min
   FROM port_event_clean3
   ORDER BY visit_id, timestamp
),

-- find rows to be removed (gap between consecutive stops < 30 minutes at the same anchorage)
port_event_short_gap AS (
   SELECT
      *,
      CASE
         WHEN LEAD(gap_min, 0) OVER (PARTITION BY visit_id ORDER BY timestamp) < 30
         THEN 1
         WHEN LEAD(gap_min, 1) OVER (PARTITION BY visit_id ORDER BY timestamp) < 30
         THEN 1
      END remove
   FROM port_event_gap
   ORDER BY visit_id, timestamp
),

-- join two consecutive stops by removing flagged rows
port_event_joined AS (
   SELECT *
   FROM port_event_short_gap
   WHERE remove IS NULL
   ORDER BY visit_id, timestamp
),

-- port stop duration
port_event_duration AS (
   SELECT
      year,
      date,
      start_timestamp,
      end_timestamp,
      anchorage_id,
      visit_id,
      vessel_id,
      CASE
         WHEN event_type = 'PORT_STOP_BEGIN' THEN timestamp
      END AS stop_begin_time,

      CASE
         WHEN LEAD(event_type, 1) OVER(PARTITION BY visit_id ORDER BY timestamp) = 'PORT_STOP_END'
         THEN LEAD(timestamp, 1) OVER(PARTITION BY visit_id ORDER BY timestamp)
      END AS stop_end_time
   FROM port_event_joined
   ORDER BY visit_id, timestamp
),
port_event_duration2 AS (
   SELECT
      *,
      TIMESTAMP_DIFF(stop_end_time, stop_begin_time, MINUTE) AS duration_min
   FROM port_event_duration
   WHERE stop_begin_time IS NOT NULL
   ORDER BY visit_id, stop_begin_time
),

-- add ssvid
ssvid_map AS (
   SELECT vessel_id, ssvid, day AS date
   FROM `world-fishing-827.pipe_production_v20190502.segment_vessel_daily_*`
   WHERE _TABLE_SUFFIX BETWEEN '20120101' AND '20191231'
),

-- Join the encounters data with the ssvid data on the same vessel_id and event day to ensure correct SSVID
port_event_ssvid AS (
   SELECT
      * EXCEPT(vessel_id, date),
      CONCAT(ssvid, start_timestamp) AS visit_start_id,
      CONCAT(ssvid, end_timestamp) AS visit_end_id
   FROM (SELECT * FROM port_event_duration2) a
   JOIN (SELECT * FROM ssvid_map) b
   ON a.vessel_id = b.vessel_id
      WHERE a.date = b.date
),

------------------------------------------
------------------------------------------
-- remove bad ssvid

-- SSVID that are likely fishing gear
likely_gear AS (
   SELECT ssvid
   FROM `world-fishing-827.gfw_research.vi_ssvid_v20200801`
   WHERE REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"(.*)([\s]+[0-9]+%)$")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"[0-9]\.[0-9]V")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"(.*)[@]+([0-9]+V[0-9]?)$")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"BOUY")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NET MARK")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NETMARK")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NETFISHING")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NET FISHING")
      OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"^[0-9]*\-[0-9]*$")
),

------------------------------------
-- This query identifies fishing vessels that meet annual quality criteria
-- e.g. not spoofing/offsetting/too many identities/etc.
fishing_vessels AS(
   SELECT
      ssvid,
      year
   FROM (
      SELECT
         ssvid,
         year
      FROM
         `world-fishing-827.gfw_research.vi_ssvid_byyear_v20200801`
      ------------------------------------
      -- Noise removal filters
      WHERE
      -- MMSI must be on best fishing list
      on_fishing_list_best
      -- MMSI cannot be used by 2+ vessels with different names simultaneously
      AND (activity.overlap_hours_multinames = 0
      OR activity.overlap_hours_multinames IS NULL)
      -- MMSI cannot be used by multiple vessels simultaneously for more than 3 days
      and activity.overlap_hours < 24*3
      -- MMSI not offsetting position
      AND activity.offsetting IS FALSE
      -- MMSI associated with 5 or fewer different shipnames
      AND 5 >= (
      SELECT
      COUNT(*)
      FROM (
      SELECT
      value,
      SUM(count) AS count
      FROM
      UNNEST(ais_identity.n_shipname)
      WHERE
      value IS NOT NULL
      GROUP BY
      value)
      WHERE
      count >= 10)
      -- MMSI not likely gear
      AND ssvid NOT IN (
      SELECT
      ssvid
      FROM
      likely_gear )
      -- MMSI vessel class can be inferred by the neural net
      AND inferred.inferred_vessel_class_byyear IS NOT NULL -- active
      -- Noise filter.
      -- MMSI active for at least 5 days and fished for at least 24 hours in the year.
      AND activity.fishing_hours > 24
      AND activity.active_hours > 24*5)
      -- Exclude MMSI that are in the manual list of problematic MMSI
      WHERE
      CAST(ssvid AS int64) NOT IN (
      SELECT
      ssvid
      FROM
      `world-fishing-827.gfw_research.bad_mmsi`
      CROSS JOIN
      UNNEST(ssvid) AS ssvid)
),

----------------------------------
-- This subquery identifies MMSI that offset a lot
nast_ssvid AS (
   SELECT
      ssvid,
      SUM( positions) positions
      FROM `world-fishing-827.gfw_research.pipe_v20200805_segs`
   WHERE (dist_avg_pos_sat_vessel_km > 3000
      AND sat_positions_known > 5)
   GROUP BY ssvid
),


------------------------------------------------
------------------------------------------------
good_ssvid AS (
   SELECT *
   FROM fishing_vessels
   WHERE ssvid NOT IN (SELECT ssvid FROM nast_ssvid)
),

port_event_good_ssvid AS (
   SELECT *
   FROM port_event_ssvid
   WHERE CONCAT(year, ssvid) IN (SELECT CONCAT(year, ssvid) FROM good_ssvid)
),


-- add flag and gear type
port_event_vessel_info AS (
   SELECT * EXCEPT(vessel_id, year, is_fishing) FROM port_event_good_ssvid AS a
   LEFT JOIN (
      SELECT
         year,
         ssvid AS vessel_id,
         IF(best.best_flag = 'UNK', ais_identity.flag_mmsi, best.best_flag) as flag,
         IF(inferred.inferred_vessel_class_ag = 'pole_and_line' AND reg_class = 'squid_jigger','squid_jigger', best.best_vessel_class) as vessel_class,
         on_fishing_list_best AS is_fishing
      FROM
         `world-fishing-827.gfw_research.vi_ssvid_byyear_v20200801`
         LEFT JOIN UNNEST(registry_info.best_known_vessel_class) as reg_class
   ) AS b
   ON a.ssvid = b.vessel_id
      WHERE a.year = b.year
      AND is_fishing IS TRUE
),


port_event_vessel_info2 AS (
   SELECT
      * EXCEPT(vessel_class),
   CASE
      WHEN vessel_class = 'trawlers' THEN 'trawlers'
      WHEN vessel_class = 'trollers' THEN 'trollers'
      WHEN vessel_class = 'driftnets' THEN 'driftnets'
      WHEN vessel_class IN ('purse_seines', 'tuna_purse_seines', 'other_purse_seine') THEN 'purse_seine'
      WHEN vessel_class = 'set_gillnets' THEN 'set_gillnet'
      WHEN vessel_class = 'squid_jigger' THEN 'squid_jigger'
      WHEN vessel_class = 'pole_and_line' THEN 'pole_and_line'
      WHEN vessel_class = 'set_longlines' THEN 'set_longline'
      WHEN vessel_class = 'pots_and_traps' THEN 'pots_and_traps'
      WHEN vessel_class = 'drifting_longlines' THEN 'drifting_longline'
      ELSE NULL
   END AS vessel_class
   FROM port_event_vessel_info
),

-- add anchorage iso3
anchorage_iso3 AS (
   SELECT
      s2id,
      iso3 AS port_iso3,
      label AS port,
      at_dock
   FROM `world-fishing-827.gfw_research.named_anchorages`
),
port_event_anchorage_info AS (
   SELECT * EXCEPT(s2id) FROM port_event_vessel_info2 AS a
   LEFT JOIN (SELECT * FROM anchorage_iso3) AS b
   ON a.anchorage_id = b.s2id
   WHERE at_dock
   ORDER BY visit_id, stop_begin_time
),

-- clean up!
port_event_clean4 AS (
   SELECT
      visit_start_id,
      visit_end_id,
      anchorage_id,
      visit_id,
      stop_begin_time,
      stop_end_time,
      duration_min,
      ssvid,
      flag,
      vessel_class,
      port_iso3,
      port,
      at_dock
   FROM port_event_anchorage_info
   WHERE duration_min IS NOT NULL
      AND at_dock
   GROUP BY
      visit_start_id,
      visit_end_id,
      anchorage_id,
      visit_id,
      stop_begin_time,
      stop_end_time,
      duration_min,
      ssvid,
      flag,
      vessel_class,
      port_iso3,
      port,
      at_dock
   ORDER BY visit_id, stop_begin_time
),


---------------------------------
-- clean up voyages
---------------------------------
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


-- match with voyages to remove some visits
voyages AS (
   SELECT
      CONCAT(ssvid, trip_start) AS trip_start_id,
      CONCAT(ssvid, trip_end) AS trip_end_id
   FROM generate_final_trips
),

-- get stop >= 30 min
SELECT *
FROM port_event_clean4
WHERE (visit_start_id IN (SELECT trip_end_id FROM voyages)
   OR visit_end_id IN (SELECT trip_start_id FROM voyages))
   AND duration_min >= 30
