with

port_visit as (
   select
      vessel_id,
      start_timestamp,
      extract(date from start_timestamp) as date,
      extract(year from start_timestamp) as year,
      start_lat,
      start_lon,
      start_anchorage_id,
      end_timestamp,
      end_lat,
      end_lon,
      end_anchorage_id,
      label,
      iso3
   from `world-fishing-827.pipe_production_v20190502.port_visits_*`
   as a
   left join (
      select
         s2id,
         label,
         iso3
      from
         `world-fishing-827.anchorages.named_anchorages_v20201104`
   ) as b
   on a.start_anchorage_id = b.s2id
   where _table_suffix between '20150101' and '20201231'
),


-- add ssvid
ssvid as (
   select
      ssvid,
      vessel_id as x1,
      day as x2
   from `world-fishing-827.pipe_production_v20190502.segment_vessel_daily_*`
   where _table_suffix between '20150101' and '20201231'
),


-- add trip id (ssvid + trip start time)
port_visit_ssvid as (
   select
      * except (x1, x2),
      concat(ssvid, start_timestamp) as visit_start_id,
      concat(ssvid, end_timestamp) as visit_end_id
   from port_visit as a
   left join (select * from ssvid) as b
   on a.vessel_id = b.x1
      and a.date = b.x2
),


-- voyages
voyages as (
   select
      concat(ssvid, trip_start) as trip_start_id,
      concat(ssvid, trip_end) as trip_end_id
   from `gfwanalysis.GFW_trips.updated_voyages2`
),


port_visit_clean as (
   select *
   from port_visit_ssvid
   where visit_start_id in (select trip_end_id from voyages)
      or visit_end_id in (select trip_start_id from voyages)
),


-- add vessel info
port_visit_vessel_info as (
   select * except(x1, x2) from port_visit_clean as a
   left join (
      select
         year as x1,
         ssvid as x2,
         IF(best.best_flag = 'UNK', ais_identity.flag_mmsi, best.best_flag) as flag,
         IF(inferred.inferred_vessel_class_ag = 'pole_and_line' and reg_class = 'squid_jigger','squid_jigger', best.best_vessel_class) as vessel_class,
         on_fishing_list_best as is_fishing
      from
         `world-fishing-827.gfw_research.vi_ssvid_byyear_v20200801`
         left join UNNEST(registry_info.best_known_vessel_class) as reg_class
   ) as b
   on a.ssvid = b.x2
      and a.year = b.x1
),


-- add flag group & gear
port_visit_flag as (
   select
      * except(vessel_class),
      case
         when flag in ('ATG','BRB','CYM','LBR','VCT','VUT') then 'group1'
         when flag in ('BHS','BHR','BLZ','BOL','BRN','KHM','CYP','GNQ','GAB','GEO','HND','KIR','MDG','MLT',
            'MHL','PAN','PRT','KNA','WSM','SLE','LKA','TON','TZA') then 'group2'
         when flag in ('ALB','DZA','AGO','AIA','ARG','AUS','AZE','BGD','BEL','BMU','BRA','BGR','CPV',
            'CMR','CAN','CHL','HKG','TWN','COL','COD','CRI','HRV','CUB','DNK','DJI','ECU',
            'EGY','ERI','EST','ETH','FJI','FIN','FRA','GMB','DEU','GHA','GRC','GRL','GRD',
            'GTM','GUY','ISL','IND','IDN','IRN','IRQ','IRL','ISR','ITA','JPN','JOR','KAZ',
            'KEN','PRK','KOR','KWT','LAO','LVA','LBN','LBY','LTU','LUX','MYS','MDV','MRT',
            'MUS','MEX','MNE','MAR','MOZ','MMR','NAM','NLD','NZL','NGA','NOR','OMN','PAK',
            'PNG','PRY','PER','PHL','POL','QAT','RUS','SAU','SEN','SYC','SGP','SVN','ZAF',
            'ESP','SDN','SUR','SWE','CHE','SYR','THA','TTO','TUN','TUR','TKM','UKR','ARE',
            'GBR','USA','URY','VEN','VNM','YEM') then 'group3'
         when flag = 'CHN' then 'china'
         when flag is null then null
         else 'other'
      end as flag_group,
      if(vessel_class in ('purse_seines', 'tuna_purse_seines', 'other_purse_seines'), 'purse_seine', vessel_class) as vessel_class
   from
      port_visit_vessel_info
),


-- encountered?
port_visit_encountered as (
   select * from port_visit_flag as a
   left join (
      select
         concat(ssvid, trip_end) as trip_end_id,
         is_encountered
      from `gfwanalysis.GFW_trips.voyages_encountered`) as b
   on a.visit_start_id = b.trip_end_id
),


port_visit_encountered_clean as (
   select
      year,
      ssvid,
      start_timestamp,
      end_timestamp,
      start_anchorage_id,
      label as port_name,
      iso3 as port_iso3,
      flag,
      flag_group,
      is_fishing,
      is_encountered,
      vessel_class
   from
      port_visit_encountered
   group by
      year,
      ssvid,
      start_timestamp,
      end_timestamp,
      start_anchorage_id,
      label,
      iso3,
      flag,
      flag_group,
      is_fishing,
      is_encountered,
      vessel_class
),


-- good ssvid
good_ssvid  as (
   select ssvid, year
   from (
      select ssvid, year
      from `world-fishing-827.gfw_research.vi_ssvid_byyear_v20200801`
      ------------------------------------
      -- Noise removal filters
      where
      -- MMSI cannot be used by 2+ vessels with different names simultaneously
         (activity.overlap_hours_multinames = 0
            or activity.overlap_hours_multinames is null)
      -- MMSI cannot be used by multiple vessels simultaneously for more than 3 days
         and activity.overlap_hours < 24*3
      -- MMSI not offsetting position
         and activity.offsetting is false
      -- MMSI associated with 5 or fewer different shipnames
         and 5 >= (
            select count(*)
            from (
               select value, sum(count) as count
               from unnest(ais_identity.n_shipname)
               where value is not null
               group by value
            )
            where count >= 10
         )
   )
   where
      cast(ssvid as int64) not in (
      select ssvid
      from `world-fishing-827.gfw_research.bad_mmsi`
      cross join unnest(ssvid) as ssvid)
),


port_visit_good_ssvid as (
    select *
    from port_visit_encountered_clean
    where (year = 2015 or year = 2017)
       and concat(year, ssvid) in (select concat(year, ssvid) from good_ssvid)
),


-- mmsi that are found both in 2015 and 2017
ssvid_keep as (
    select ssvid
    from `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210301`
    where year = 2015

    intersect distinct

    select ssvid
    from `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210301`
    where year = 2017
)


select *
from port_visit_good_ssvid
where ssvid in (select ssvid from ssvid_keep)
