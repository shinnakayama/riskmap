#standardSQL

WITH

-- Nate's update on voyages
updated_voyages AS (
   SELECT
      *
   FROM `gfwanalysis.GFW_trips.updated_voyages`
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
   FROM updated_voyages
),


-- good ssvid
good_ssvid AS (
   SELECT *
   FROM `gfwanalysis.GFW_trips.good_ssvid`
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
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      start_anchorage_lat,
      start_anchorage_lon,
      start_anchorage_label,
      start_anchorage_iso3,
      start_distance_from_shore_m,
      trip_end_anchorage_id,
      end_anchorage_lat,
      end_anchorage_lon,
      end_anchorage_label,
      end_anchorage_iso3,
      end_distance_from_shore_m,
      trip_duration_hr,
      port_stop_duration_hr,
      flag,
      vessel_class,
      is_fishing
   FROM
      voyages_with_vessel_info
   GROUP BY
      trip_id,
      ssvid,
      trip_start,
      trip_end,
      trip_start_anchorage_id,
      start_anchorage_lat,
      start_anchorage_lon,
      start_anchorage_label,
      start_anchorage_iso3,
      start_distance_from_shore_m,
      trip_end_anchorage_id,
      end_anchorage_lat,
      end_anchorage_lon,
      end_anchorage_label,
      end_anchorage_iso3,
      end_distance_from_shore_m,
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
      trip_id,
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
         WHEN vessel_class IN ('purse_seines', 'tuna_purse_seines') THEN 'purse_seine'
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
