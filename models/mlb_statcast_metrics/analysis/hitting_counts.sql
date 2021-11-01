{{
    config(
      materialized= 'table',
      unique_key= 'batter_id'
    )
}}

WITH cte_plate_appearance AS (
  SELECT * FROM {{ref('stg_plate_appearance')}}
  ),
cte_counts AS (
  SELECT
    DISTINCT batter_id
   ,batter_full_name
   ,game_year
   ,SUM(is_plate_appearance_bool) AS pa
   ,SUM(is_at_bat_bool) AS ab
   ,( SUM(single) + SUM(double) + SUM(triple) + SUM(home_run) ) AS hit
   ,SUM(hbp) AS hit_by_pitch
   ,SUM(walk) AS walk
   ,SUM(single) AS single
   ,SUM(double) AS double
   ,SUM(triple) AS triple
   ,SUM(home_run) AS home_run
  FROM cte_plate_appearance
  GROUP BY 1,2,3
  ),
  cte_final AS (
  SELECT * FROM cte_counts
  )

SELECT * FROM cte_final