#standardsql

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
 SELECT *
   FROM (
     SELECT
     ssvid,
     vessel_ids,
     CAST(IF(trip_start < TIMESTAMP("1900-01-01"), NULL, trip_start) AS TIMESTAMP) AS trip_start,
     CAST(IF(trip_end > TIMESTAMP("2099-12-31"), NULL, trip_end) AS TIMESTAMP) AS trip_end,
     trip_start_anchorage_id,
     trip_end_anchorage_id
     FROM (
       SELECT *
         FROM `world-fishing-827.gfw_research.voyages_no_overlapping_short_seg_v20200819`
       WHERE trip_start <= maximum()
       AND trip_end >= minimum()
       AND trip_start_anchorage_id != "10000001"
       AND trip_end_anchorage_id != "10000001"))
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
 IF (trip_end_anchorage_id IN (
   SELECT anchorage_id FROM panama_canal_ids ),
   TRUE, FALSE ) current_end_is_panama,
 IF (trip_start_anchorage_id IN (
   SELECT anchorage_id FROM panama_canal_ids ),
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
 --       IF (current_start_is_panama AND prev_end_is_panama, NULL, trip_start) AS block_start,
 --       IF (current_end_is_panama AND next_start_is_panama, NULL, trip_end) AS block_end
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
   FROM updated_pan_voyages)
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
 SELECT *,
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
 WHERE
 ((trip_end >= minimum()
   OR trip_end IS NULL) )
 AND (trip_end_anchorage_id = "ACTIVE_VOYAGE"
      OR trip_duration_hr > min_trip_duration()
      OR trip_start_anchorage_id = "NO_PREVIOUS_DATA")
 AND (trip_start <= maximum()
      OR trip_start IS NULL)
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),

-- change NEWPORT to NEWPORT (OREGON) to distinguish from NEWPORT (RHODE ISLAND)
-- change STONINGTON to STONINGTON (MAINE) to distinguish from STONINGTON (CONNECTICUT)
updated_voyages2 AS (
   SELECT
      * EXCEPT (start_anchorage_label, end_anchorage_label),
      CASE
         WHEN trip_start_anchorage_id IN ('54c1d7b1','54c1d7a5','54c1d7b7','54c1d7af','54c1d7b5','54c1d7b3','54c1d7ad',
         '54c1d64d','54c1d653','54c1d655','54c1d72f','54906be3') THEN 'NEWPORT (OREGON)'
         WHEN trip_start_anchorage_id IN ('4cac27cf','4cac2773') THEN 'STONINGTON (MAINE)'
         ELSE start_anchorage_label
      END AS start_anchorage_label,
      CASE
         WHEN trip_end_anchorage_id IN ('54c1d7b1','54c1d7a5','54c1d7b7','54c1d7af','54c1d7b5','54c1d7b3','54c1d7ad',
         '54c1d64d','54c1d653','54c1d655','54c1d72f','54906be3') THEN 'NEWPORT (OREGON)'
         WHEN trip_start_anchorage_id IN ('4cac27cf','4cac2773') THEN 'STONINGTON (MAINE)'
         ELSE end_anchorage_label
      END AS end_anchorage_label
   FROM generate_final_trips
),


---------------------------------------
-- 'fishing_vessels_no_offsetting_spoofing.sql' by Tylor
-- 1) Identify MMSI that are not likely fishing gear using shipname
-- 2) Identify active fishing vessels that do not spoof/offsett
-- 3) Remove MMSI that have been manually identified as problematic
---------------------------------------

-- EXAMPLE QUERY: IDENTIFY ACTIVE NON-OFFSETTING NON-SPOOFING FISHING VESSELS
-- DESCRIPTION:
-- This query uses the vessel info table to identify active
-- MMSI that are likely fishing vessels who are not spoofing
-- or offsetting their location
-- 1) Identify MMSI that are not likely fishing gear using shipname
-- 2) Identify active fishing vessels that do not spoof/offsett
-- 3) Remove MMSI that have been manually identified as problematic

-- SSVID that are likely fishing gear
likely_gear AS (
SELECT
 ssvid
FROM
 `world-fishing-827.gfw_research.vi_ssvid_v20200801`
WHERE
 REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"(.*)([\s]+[0-9]+%)$")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"[0-9]\.[0-9]V")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"(.*)[@]+([0-9]+V[0-9]?)$")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"BOUY")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NET MARK")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NETMARK")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NETFISHING")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"NET FISHING")
 OR REGEXP_CONTAINS(ais_identity.shipname_mostcommon.value, r"^[0-9]*\-[0-9]*$")),

------------------------------------
-- This query identifies fishing vessels that meet annual quality criteria
-- e.g. not spoofing/offsetting/too many identities/etc.
fishing_vessels AS(
SELECT
 ssvid,
 year,
 geartype,
 flag
FROM (
 SELECT
   ssvid,
   year,
   best.best_vessel_class geartype,
   best.best_flag flag
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
   UNNEST(ssvid) AS ssvid)),

----------------------------------
-- This subquery identifies MMSI that offsett a lot
nast_ssvid AS (
SELECT
ssvid,
SUM( positions) positions
FROM `world-fishing-827.gfw_research.pipe_v20190502_segs`
WHERE
(( dist_avg_pos_sat_vessel_km > 3000
AND sat_positions_known > 5)
)
GROUP BY ssvid
),

--------------------
-- Return final list of good fishing vessels
-- good ssvid
good_ssvid AS (
   SELECT *
   FROM fishing_vessels
   WHERE ssvid NOT IN (SELECT ssvid FROM nast_ssvid)
),

-- filter for good ssvid
voyages_with_good_ssvid AS (
   SELECT *,
   EXTRACT(YEAR FROM trip_start) AS start_year
   FROM updated_voyages2
   WHERE EXTRACT(DATE FROM trip_start) >= '2012-01-01'
      AND EXTRACT(DATE FROM trip_start) <= '2019-12-31'
      AND ssvid IN (SELECT ssvid FROM good_ssvid)
),

-- add vessel information (flag & vessel class)
-- select only fishing vessels
voyages_with_vessel_info AS (
   SELECT
      * EXCEPT(vessel_id, start_year, year)
   FROM voyages_with_good_ssvid AS a
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
   WHERE start_year = year
      AND is_fishing IS TRUE
),

voyages_with_vessel_info_clean AS (
   SELECT
      gfw_trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      start_anchorage_lat,
      start_anchorage_lon,
      start_anchorage_label,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_lat,
      end_anchorage_lon,
      end_anchorage_label,
      end_anchorage_iso3,
      trip_duration_hr,
      port_stop_duration_hr,
      flag,
      vessel_class,
      is_fishing
   FROM
      voyages_with_vessel_info
   GROUP BY
      gfw_trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      start_anchorage_lat,
      start_anchorage_lon,
      start_anchorage_label,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_lat,
      end_anchorage_lon,
      end_anchorage_label,
      end_anchorage_iso3,
      trip_duration_hr,
      port_stop_duration_hr,
      flag,
      vessel_class,
      is_fishing
),


-- add flag state name
country_codes AS (
   SELECT iso3, country_name
   FROM `world-fishing-827.gfw_research.country_codes`
   GROUP BY 1,2
),
vessel_info_flag_state AS (
   SELECT * EXCEPT(iso3) FROM voyages_with_vessel_info_clean AS a
   LEFT JOIN (
      SELECT
         iso3,
         country_name
      FROM country_codes
   ) AS b
   ON a.flag = b.iso3
),


-- add flag of convenience
--flag_type: high risk, low risk, China, no known risk
vessel_info_foc AS (
   SELECT
      *,
      CASE
         WHEN flag IN ('ATG','BRB','CYM','LBR','VCT','VUT') THEN 'group1'
         WHEN flag IN ('BHS','BHR','BLZ','BOL','BRN','KHM','CYP','GNQ','GAB','GEO','HND','KIR','MDG','MLT',
            'MHL','PAN','PRT','KNA','WSM','SLE','LKA','TON','TZA') THEN 'group2'
         WHEN flag IN ('ALB','DZA','AGO','AIA','ARG','AUS','AZE','BGD','BEL','BMU','BRA','BGR','CPV',
            'CMR','CAN','CHL','HKG','TWN','COL','COD','CRI','HRV','CUB','DNK','DJI','ECU',
            'EGY','ERI','EST','ETH','FJI','FIN','FRA','GMB','DEU','GHA','GRC','GRL','GRD',
            'GTM','GUY','ISL','IND','IDN','IRN','IRQ','IRL','ISR','ITA','JPN','JOR','KAZ',
            'KEN','PRK','KOR','KWT','LAO','LVA','LBN','LBY','LTU','LUX','MYS','MDV','MRT',
            'MUS','MEX','MNE','MAR','MOZ','MMR','NAM','NLD','NZL','NGA','NOR','OMN','PAK',
            'PNG','PRY','PER','PHL','POL','QAT','RUS','SAU','SEN','SYC','SGP','SVN','ZAF',
            'ESP','SDN','SUR','SWE','CHE','SYR','THA','TTO','TUN','TUR','TKM','UKR','ARE',
            'GBR','USA','URY','VEN','VNM','YEM') THEN 'group3'
         WHEN flag = 'CHN' THEN 'china'
         WHEN flag IS NULL THEN NULL
         ELSE 'other'
      END AS flag_group

   FROM
      vessel_info_flag_state
),

-------------------------
-- lookup table for GFW anchorage -- COS port

-- GFW anchorage table
GFW_anchorage AS (
   SELECT
      label,
      s2id,
      iso3,
      lat AS anchorage_lat,
      lon AS anchorage_lon,
      ST_GEOGPOINT(lon, lat) AS anchorage_coords
   FROM `world-fishing-827.gfw_research.named_anchorages`
),


-- this is a table that count of not associated / low risk / medium risk /high risk
-- for each port
COS_port AS (
   SELECT
      not_associated,
      iuu_low, iuu_med, iuu_high,
      la_low, la_med, la_high,
      lat AS port_lat,
      lon AS port_lon,
      ST_GEOGPOINT(lon, lat) AS port_coords,
      port_id
   FROM
      `gfwanalysis.qualtrics_survey.port_risk`
),


-- remove duplicated COS ports (same port label in GFW data as of 10.10.2020)
COS_port2 AS (
   SELECT * FROM COS_port
   WHERE port_id NOT IN ('p57','p504', 'p505','p768','p609','p317','p51','p74','p52','p63','p2', 'p173')
),


-- match!
GFW_anchorage_with_port AS (
   SELECT
      s2id,
      label,
      iso3,
      ARRAY_AGG(port_id ORDER BY ST_DISTANCE(anchorage_coords, port_coords) LIMIT 1) [ORDINAL(1)] AS port_id,
   FROM GFW_anchorage
   JOIN COS_port2
   ON ST_DWITHIN(anchorage_coords, port_coords, 3000) -- search within 3 km
   GROUP BY s2id, label, iso3
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
      iso3,
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

port_summary2 AS (
   SELECT
      * EXCEPT (label),
      CASE
         WHEN port_id = 'p610' THEN 'NEWPORT (OREGON)'
         WHEN port_id = 'p641' THEN 'STONINGTON (MAINE)'
         ELSE label
      END AS label
   FROM port_summary
),


-------------------------------
-- add COS port_id to voyage
voyages_from_port AS (
   SELECT * EXCEPT(iso3, label) FROM vessel_info_foc AS a
   LEFT JOIN (
      SELECT
         port_id AS from_port_id,
         iso3,
         label
      FROM port_summary2) AS b
   ON a.start_anchorage_label = b.label
      AND a.start_anchorage_iso3 = b.iso3
),
voyages_to_port AS (
   SELECT * EXCEPT(iso3, label) FROM voyages_from_port AS a
   LEFT JOIN (
      SELECT
         port_id AS to_port_id,
         iso3,
         label
      FROM port_summary2) AS b
   ON a.end_anchorage_label = b.label
      AND a.end_anchorage_iso3 = b.iso3
),


-- trip summary
trip_summary AS (
   SELECT
      gfw_trip_id,
      trip_start_anchorage_id,
      start_anchorage_label,
      trip_end_anchorage_id,
      end_anchorage_label,
      ssvid,
      trip_start,
      trip_end,
      from_port_id,
      to_port_id,
      flag_group,
      flag,
      COALESCE (
         CASE WHEN trip_duration_hr < 24*30*1 THEN 'less_than_1m' ELSE NULL END,
         CASE WHEN trip_duration_hr < 24*30*3 THEN '1_3m' ELSE NULL END,
         CASE WHEN trip_duration_hr < 24*30*6 THEN '3_6m' ELSE NULL END,
         CASE WHEN trip_duration_hr < 24*30*12 THEN '6_12m' ELSE NULL END,
         CASE WHEN trip_duration_hr >= 24*30*12 THEN '12m_and_more' ELSE NULL END
      ) AS time_at_sea,
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
      END AS vessel_class,
   FROM voyages_to_port
),


-- add risk vote number of arrival port
trip_to_risk AS (
   SELECT * EXCEPT (port_id) FROM trip_summary AS a
   LEFT JOIN (
      SELECT
         port_id,
         iuu_low AS iuu_low_to,
         iuu_med AS iuu_med_to,
         iuu_high AS iuu_high_to,
         not_associated AS iuu_no_to,
         la_low AS la_low_to,
         la_med AS la_med_to,
         la_high AS la_high_to,
         not_associated AS la_no_to
      FROM COS_port
   ) AS b
   ON a.to_port_id = b.port_id
),


-- add risk vote number of departure port
trip_from_risk AS (
   SELECT * EXCEPT (port_id) FROM trip_to_risk AS a
   LEFT JOIN (
      SELECT
         port_id,
         iuu_low AS iuu_low_from,
         iuu_med AS iuu_med_from,
         iuu_high AS iuu_high_from,
         not_associated AS iuu_no_from,
         la_low AS la_low_from,
         la_med AS la_med_from,
         la_high AS la_high_from,
         not_associated AS la_no_from
      FROM COS_port
   ) AS b
   ON a.from_port_id = b.port_id
)


SELECT *
FROM trip_from_risk
