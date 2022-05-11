{{
    config(
      materialized= 'table',
      unique_key= 'batter_id'
    )
}}

with _stg_plate_appearance as (
  select * from {{ref('stg_plate_appearance')}}
),
_woba_fip_cnst as (
  select * from {{ref('woba_fip_cnst')}}
),
_slash_line as (
  select
     batter_id
    ,batter_full_name
    ,bs.game_year as game_year
    ,round( sum(ab_safe_or_out_bool) / sum(is_at_bat_bool) , 3) as batting_avg
    ,round( sum(pa_safe_or_out_bool) / sum(is_plate_appearance_bool), 3) as obp --on base percentage
    ,round( sum(bases_for_slg) / sum(is_at_bat_bool), 3) as slg_percentage -- slugging percentage
    ,obp + slg_percentage as ops -- on base percentage plus slugging percentage
    --woba (weighted on base average) uses a predetermined scale (varies by season) to weight significance of outcome.
    ,round( ( (sum(walk) * min(wf.wbb)) + (sum(hbp) * min(wf.whbp) )  + (sum(single) * min( wf.w1b)) + (sum(double) * min(wf.w2b)) + (sum(triple) * min( wf.w3b))
      + (sum(home_run) * min(wf.whr)) ) / ( (sum(is_at_bat_bool) + sum(walk) - sum(ibb) + sum(sf) + sum(hbp) ) ), 3 ) as woba_pl
    --wraa (weight runs above average) - how many runs a player adds to the team compared to the average player (scored at 0)
    ,round( ( ( woba_pl - min(wf.woba) ) / min(wf.wobascale) ) * sum(is_plate_appearance_bool), 3 ) as wraa
  from _stg_plate_appearance bs
  join _woba_fip_cnst wf
    on bs.game_year = wf.season
  join stg_game_count gc
    on bs.game_pk = gc.game_pk
   and bs.game_year = gc.game_year
   and bs.batting_team = gc.team
  group by 1,2,3
  having sum(is_plate_appearance_bool) >= (max(gc.game_count) * 3.1)
  order by 6 desc
  ), 
_final as (
  select * from _slash_line
  )

select * from _final