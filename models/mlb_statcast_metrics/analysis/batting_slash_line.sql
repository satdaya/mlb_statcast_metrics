{{
    config(
      materialized= 'table',
      unique_key= 'batter_id'
    )
}}

WITH cte_stg_plate_appearance AS (
  SELECT * FROM {{ref('stg_plate_appearance')}}
),
cte_woba_fip_cnst AS (
  SELECT * FROM {{ref('woba_fip_cnst')}}
),
cte_slash_line AS (
  SELECT
     batter_id
    ,batter_full_name
    ,bs.game_year AS game_year
    ,ROUND( SUM(ab_safe_or_out_bool) / SUM(is_at_bat_bool) , 3) AS batting_avg
    ,ROUND( SUM(pa_safe_or_out_bool) / SUM(is_plate_appearance_bool), 3) AS obp --on base percentage
    ,ROUND( SUM(bases_for_slg) / SUM(is_at_bat_bool), 3) as slg_percentage -- slugging percentage
    ,obp + slg_percentage AS ops -- on base percentage plus slugging percentage
    --wOBA (weighted on base average) uses a predetermined scale (varies by season) to weight significance of outcome.
    ,ROUND( ( (SUM(walk) * MIN(wf.wbb)) + (SUM(hbp) * MIN(wf.whbp) )  + (SUM(single) * MIN( wf.w1b)) + (SUM(double) * MIN(wf.w2b)) + (SUM(triple) * MIN( wf.w3b))
      + (SUM(home_run) * MIN(wf.whr)) ) / ( (SUM(is_at_bat_bool) + SUM(walk) - SUM(ibb) + SUM(sf) + SUM(hbp) ) ), 3 ) AS wOBA_pl
    --wRAA (weight runs above average) - how many runs a player adds to the team compared to the average player (scored at 0)
    ,ROUND( ( ( wOBA_pl - MIN(wf.woba) ) / MIN(wf.wobascale) ) * SUM(is_plate_appearance_bool), 3 ) as wRAA
  FROM cte_stg_plate_appearance bs
  JOIN cte_woba_fip_cnst wf
    ON bs.game_year = wf.season
  JOIN stg_game_count gc
    ON bs.game_pk = gc.game_pk
   AND bs.game_year = gc.game_year
   AND bs.batting_team = gc.team
  GROUP BY 1,2,3
  HAVING SUM(is_plate_appearance_bool) >= (MAX(gc.game_count) * 3.1)
  ORDER BY 6 DESC
  ), 
cte_final AS (
  SELECT * FROM cte_slash_line
  )

SELECT * FROM cte_final