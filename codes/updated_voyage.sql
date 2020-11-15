#standardsql

WITH

trip_ids AS (
   SELECT
      *
   FROM (
      SELECT
         ssvid,
         trip_id,
         CAST(IF(trip_start = TIMESTAMP("0001-02-03"), NULL, trip_start) AS TIMESTAMP) AS trip_start,
         CAST(IF(trip_end = TIMESTAMP("9999-09-09"), NULL, trip_end) AS TIMESTAMP) AS trip_end,
         trip_start_anchorage_id,
         trip_end_anchorage_id
      FROM (
         SELECT * FROM (
            SELECT * EXCEPT(vessel_ids)
            FROM `world-fishing-827.pipe_production_v20200805.voyages`
            WHERE trip_start <= TIMESTAMP("2019-12-31") AND trip_end >= TIMESTAMP("2012-01-01")
         )
      )
   )
),


panama_canal_ids AS (
   SELECT s2id AS anchorage_id
   FROM `world-fishing-827.gfw_research.named_anchorages`
   WHERE sublabel = 'PANAMA CANAL'
),

--#
--##
--# Add start anchorage iso3
--# to trips
--##
--#
--#
add_trip_start_iso3 AS (
   SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      b.iso3 start_anchorage_iso3,
      trip_end_anchorage_id,
      TIMESTAMP_DIFF(trip_end, trip_start, SECOND)/3600 AS trip_duration_hr
   FROM
      (SELECT * FROM trip_ids) a
   LEFT JOIN
      (SELECT * FROM `world-fishing-827.gfw_research.named_anchorages`) b
   ON a.trip_start_anchorage_id = b.s2id
   group by 1,2,3,4,5,6,7,8
),

--#
--#
--##
--# Add end anchorage iso3
--# to trips
--##
--#
--#
add_trip_end_iso3 AS (
   SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      b.iso3 AS end_anchorage_iso3,
      trip_duration_hr
   FROM
      (SELECT * FROM add_trip_start_iso3) a
   LEFT JOIN (SELECT * FROM `world-fishing-827.gfw_research.named_anchorages`) b
   ON a.trip_end_anchorage_id = b.s2id
   group by 1,2,3,4,5,6,7,8,9
),


--#
--##
--# identify if the current, previous, or
--# next port stops occur in Panama
--# Testing if removing these (most are associated
--# with the transit through the canal) makes
--# for better visits statistics
--##
--#

is_end_port_pan AS (
   SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id ,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_iso3,
      current_is_panama,
      LAG(current_is_panama, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS prev_is_panama,
      LEAD(current_is_panama, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_is_panama
   FROM (
      SELECT
         trip_id,
         ssvid,
         trip_start,
         trip_end ,
         trip_start_anchorage_id ,
         start_anchorage_iso3,
         end_anchorage_iso3,
         trip_end_anchorage_id,
         IF(trip_end_anchorage_id IN (SELECT anchorage_id FROM panama_canal_ids), TRUE, FALSE) current_is_panama
      FROM add_trip_end_iso3)
),

--#
--##
--# label trips that involve a Panama
--# start or stop
--##
--#


label_panama AS (
   SELECT
      ssvid,
      trip_id,
      trip_start_anchorage_id,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      end_anchorage_iso3,
      trip_start,
      trip_end,
      next_voyage_end,
      LEAD(trip_end_anchorage_id, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_end_anchorage_id,
      LAG(trip_start_anchorage_id, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS prev_start_anchorage_id,
      current_is_panama,
      prev_is_panama,
      trip_type
   FROM (
      SELECT
         *,
         LEAD(trip_end, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_end
      FROM (
         SELECT
            ssvid,
            trip_id,
            trip_start,
            trip_end,
            trip_start_anchorage_id,
            start_anchorage_iso3,
            trip_end_anchorage_id,
            end_anchorage_iso3,
            current_is_panama,
            prev_is_panama,
            CASE
               WHEN current_is_panama IS FALSE AND (prev_is_panama IS NULL OR prev_is_panama IS FALSE) THEN "good_trip"
               WHEN current_is_panama IS TRUE AND (prev_is_panama IS FALSE OR prev_is_panama IS NULL) THEN "start"
               WHEN current_is_panama IS TRUE AND prev_is_panama IS TRUE THEN "remove"
               WHEN current_is_panama IS FALSE AND (prev_is_panama IS TRUE or trip_start_anchorage_id IN (SELECT anchorage_id FROM panama_canal_ids)) THEN "end"
               ELSE NULL
            END AS trip_type
         FROM
         is_end_port_pan
      )
   WHERE trip_type != "remove"
   )
),


-- #
-- #
-- ###
-- # update with appropriate trip start/trip ends
-- # anchorage_ids, voyage duration, and port duration
-- # removing intermediate stops that occur in Panama
-- ###
-- #
-- #
updated_pan_voyages AS (
   SELECT
      trip_id,
      ssvid,
   CASE
      WHEN trip_type = "good_trip" THEN trip_start
      WHEN trip_type = "start" THEN trip_start
      ELSE NULL
   END AS trip_start,
   CASE
      WHEN trip_type = "good_trip" THEN trip_start_anchorage_id
      WHEN trip_type = "start" THEN trip_start_anchorage_id
      ELSE NULL
   END AS trip_start_anchorage_id,
   CASE
      WHEN trip_type = "good_trip" THEN trip_end
      WHEN trip_type = "start" THEN next_voyage_end
      ELSE NULL
   END AS trip_end,
   CASE
      WHEN trip_type = "good_trip" THEN trip_end_anchorage_id
      WHEN trip_type = "start" THEN next_end_anchorage_id
      ELSE NULL
   END AS trip_end_anchorage_id
   FROM
      label_panama
   WHERE trip_type != "end"
),

-- #
-- #
-- ###
-- # NEXT: Identify **too short** port stops
-- # Add port stop duration
-- ###
-- #
-- #

add_port_stop_duration AS (
   SELECT
      *,
      TIMESTAMP_DIFF(next_voyage_start, trip_end, SECOND)/3600 port_stop_duration_hr
   FROM (
   SELECT
   *,
   LEAD(trip_start, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_start
   FROM
   updated_pan_voyages)
),



--#
--##
--# identify if the current, previous, or
--# next port stops are *too* short
--# in this case less than 3 hours
--##
--#

is_port_too_short AS (
   SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id ,
      trip_end_anchorage_id,
      current_port_too_short,
      port_stop_duration_hr,
      LAG(current_port_too_short, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS prev_port_too_short,
      LEAD(current_port_too_short, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_port_too_short
   FROM (
      SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end ,
      trip_start_anchorage_id ,
      trip_end_anchorage_id,
      port_stop_duration_hr,
      IF(port_stop_duration_hr < 0.5, TRUE, FALSE) current_port_too_short
   FROM add_port_stop_duration)
),

--#
--###
--# Label voyages as ones that are
--# "good", ones where we want to use
--# the "start" time, ones where we want
--# to use the "end" time, and ones that
--# we want to remove "remove"
--###
--#
--#
label_trips AS (
   SELECT
      ssvid,
      trip_id,
      trip_start_anchorage_id,
      trip_end_anchorage_id,
      trip_start,
      trip_end,
      LEAD(trip_end_anchorage_id, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_end_anchorage_id,
      LAG(trip_start_anchorage_id, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS prev_start_anchorage_id,
      current_port_too_short,
      prev_port_too_short,
      trip_type,
      port_stop_duration_hr,
      next_voyage_end,
      next_port_stop_duration_hr
   FROM (
      SELECT
         *,
         LEAD(trip_end, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_end,
         LEAD(port_stop_duration_hr, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_port_stop_duration_hr
         FROM (
         SELECT
         ssvid,
         trip_id,
         trip_start_anchorage_id,
         trip_end_anchorage_id,
         trip_start,
         trip_end,
         current_port_too_short,
         prev_port_too_short,
         CASE
            WHEN current_port_too_short IS FALSE AND (prev_port_too_short IS NULL OR prev_port_too_short IS FALSE) THEN "good_trip"
            WHEN current_port_too_short IS TRUE AND prev_port_too_short IS FALSE THEN "start"
            WHEN current_port_too_short IS TRUE AND prev_port_too_short IS TRUE THEN "remove"
            WHEN current_port_too_short IS FALSE AND prev_port_too_short IS TRUE THEN "end"
            ELSE NULL
         END AS trip_type,
         port_stop_duration_hr
      FROM
      is_port_too_short
   )
   WHERE trip_type != "remove")
),


-- #
-- #
-- ###
-- # update with appropriate trip start/trip ends
-- # anchorage_ids, voyage duration, and port duration
-- ###
-- #
-- #
updated_voyages AS (
   SELECT
      trip_id,
      ssvid,
      CASE
         WHEN trip_type = "good_trip" THEN trip_start
         WHEN trip_type = "start" THEN trip_start
         ELSE NULL
      END AS trip_start,
      CASE
         WHEN trip_type = "good_trip" THEN trip_start_anchorage_id
         WHEN trip_type = "start" THEN trip_start_anchorage_id
         ELSE NULL
      END AS trip_start_anchorage_id,
      CASE
         WHEN trip_type = "good_trip" THEN trip_end
         WHEN trip_type = "start" THEN next_voyage_end
         ELSE NULL
      END AS trip_end,
      CASE
         WHEN trip_type = "good_trip" THEN trip_end_anchorage_id
         WHEN trip_type = "start" THEN next_end_anchorage_id
         ELSE NULL
      END AS trip_end_anchorage_id,
      CASE
         WHEN trip_type = "good_trip" THEN port_stop_duration_hr
         WHEN trip_type = "start" THEN next_port_stop_duration_hr
         ELSE NULL
      END AS port_stop_duration_hr
   FROM
      label_trips
   WHERE
      trip_type != "end"
),



--#
--##
--# Add start anchorage labels
--# and long/lat to trips
--##
--#
--#
trip_start_label AS (
   SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      b.lat start_anchorage_lat,
      b.lon start_anchorage_lon,
      b.label start_anchorage_label,
      b.iso3 start_anchorage_iso3,
      b.distance_from_shore_m AS start_distance_from_shore_m,
      trip_end_anchorage_id,
      TIMESTAMP_DIFF(trip_end, trip_start, SECOND)/3600 AS trip_duration_hr,
      port_stop_duration_hr
   FROM (SELECT * FROM updated_voyages) a
   LEFT JOIN (
      SELECT * FROM `world-fishing-827.gfw_research.named_anchorages`
   ) b
   ON a.trip_start_anchorage_id = b.s2id
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),



--#
--#
--##
--# Add end anchorage labels
--# and long/lat to trips
--##
--#
--#
trip_end_label AS (
   SELECT
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      start_anchorage_lat,
      start_anchorage_lon,
      start_anchorage_label,
      start_anchorage_iso3,
      trip_end_anchorage_id,
      start_distance_from_shore_m,
      b.lat end_anchorage_lat,
      b.lon end_anchorage_lon,
      b.label end_anchorage_label,
      b.iso3 end_anchorage_iso3,
      b.distance_from_shore_m AS end_distance_from_shore_m,
      trip_duration_hr,
      port_stop_duration_hr
   FROM (SELECT * FROM trip_start_label) a
   LEFT JOIN (
      SELECT * FROM `world-fishing-827.gfw_research.named_anchorages`
   ) b
   ON a.trip_end_anchorage_id = b.s2id
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)


SELECT * FROM trip_end_label
