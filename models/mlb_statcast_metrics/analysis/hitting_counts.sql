{{
    config(
      materialized= 'table',
      unique_key= 'batter_id'
    )
}}

with _plate_appearance as (
  select * from {{ref('stg_plate_appearance')}}
  ),
_counts as (
  select
    distinct batter_id
   ,batter_full_name
   ,game_year
   ,sum(is_plate_appearance_bool) as pa
   ,sum(is_at_bat_bool) as ab
   ,( sum(single) + sum(double) + sum(triple) + sum(home_run) ) as hit
   ,sum(hbp) as hit_by_pitch
   ,sum(walk) as walk
   ,sum(single) as single
   ,sum(double) as double
   ,sum(triple) as triple
   ,sum(home_run) as home_run
  from _plate_appearance
  group by 1,2,3
  ),
_final as (
  select * from _counts
  )

select * from _final