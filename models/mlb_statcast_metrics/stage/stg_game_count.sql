{{
    config(
      materialized= 'table',
      unique_key= 'comp_key'
    )
}}

with _base_statcast as (
  select * from {{ref('base_statcast')}}
),
_home_game as (
  select
    distinct game_pk as game_pk
   ,home_team as team
   ,away_team as opponent
   ,gm_date
   ,game_year
  from _base_statcast
  ),
_away_game as (
  select
    distinct game_pk as game_pk
   ,away_team as team
   ,home_team as opponent
   ,gm_date
   ,game_year
  from _base_statcast
  ),
_stack as (
  select * 
  from _home_game
  
  union all
  
  select *
  from _away_game
  ),
_count as (
  select
    *
   ,count(game_pk) over (partition by team, game_year order by gm_date) as game_count
  from _stack
  ),
_yearly_count as (
  select
    game_year
   ,team
   ,max(game_count) as yearly_game_count
  from _count
  group by 1,2
  ),
_final as (
select
   c.game_pk
  ,c.team
  ,c.opponent
  ,c.gm_date
  ,c.game_year
  ,c.game_count
  ,y.yearly_game_count
  ,c.game_pk || c.team as comp_key
from _count c
join _yearly_count y
  on c.game_year = y.game_year
 and c.team = y.team
  )
  
select * from _final