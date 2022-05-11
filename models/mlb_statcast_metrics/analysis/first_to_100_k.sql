{{
    config(
      materialized= 'table',
      unique_key= 'batter_id'
    )
}}

with _plate_appearance as (
  select * from {{ref('stg_plate_appearance')}}
  ),
_game_count as (
  select * from {{ref('stg_game_count')}}
  ),
_strikeouts as (
  select
     distinct batter_id
    ,batter_full_name
    ,gm_date
    ,game_year
    ,sum(strikeout) over (partition by batter_id, game_year order by gm_date) as running_count
  from _plate_appearance
  ),
_first_to_hundred as (
  select
    batter_id
   ,batter_full_name
   ,game_year
   ,min(gm_date) as date_to_100_k
  from _strikeouts
  where running_count = 100
  group by 1,2,3
  order by 4 desc
  ),
_final as (
  select * from _first_to_hundred
  )

select * from _final
order by 4