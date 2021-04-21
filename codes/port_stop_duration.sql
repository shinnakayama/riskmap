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
   SELECT * EXCEPT(s2id) FROM port_event_vessel_info AS a
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


-- refine port names & gear_type
port_event_clean5 AS (
   SELECT
      * EXCEPT (port, vessel_class),
      CASE
         WHEN anchorage_id IN ('54c1d7b1','54c1d7a5','54c1d7b7','54c1d7af','54c1d7b5','54c1d7b3','54c1d7ad',
         '54c1d64d','54c1d653','54c1d655','54c1d72f','54906be3') THEN 'NEWPORT (OREGON)'
         WHEN anchorage_id IN ('4cac27cf','4cac2773') THEN 'STONINGTON (MAINE)'
         ELSE port
      END AS port,
      CASE
         WHEN vessel_class = 'trawlers' THEN 'trawlers'
         WHEN vessel_class = 'trollers' THEN 'trollers'
         WHEN vessel_class = 'driftnets' THEN 'driftnets'
         WHEN vessel_class IN ('purse_seines', 'tuna_purse_seines', 'other_purse_seines') THEN 'purse_seine'
         WHEN vessel_class = 'set_gillnets' THEN 'set_gillnet'
         WHEN vessel_class = 'squid_jigger' THEN 'squid_jigger'
         WHEN vessel_class = 'pole_and_line' THEN 'pole_and_line'
         WHEN vessel_class = 'set_longlines' THEN 'set_longline'
         WHEN vessel_class = 'pots_and_traps' THEN 'pots_and_traps'
         WHEN vessel_class = 'drifting_longlines' THEN 'drifting_longline'
         ELSE NULL
      END AS vessel_class
   FROM port_event_clean4
),


-- match with voyages to remove some visits
voyages AS (
   SELECT
      CONCAT(ssvid, trip_start) AS trip_start_id,
      CONCAT(ssvid, trip_end) AS trip_end_id
   FROM `gfwanalysis.GFW_trips.updated_voyages2`
),
port_event_clean6 as (
   SELECT *
   FROM port_event_clean5
   WHERE visit_start_id IN (SELECT trip_end_id FROM voyages)
      OR visit_end_id IN (SELECT trip_start_id FROM voyages)
)


select *
from port_event_clean6
where vessel_class is not null
  and flag is not null
