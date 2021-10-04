{{
    config(
      materialized= 'table',
      unique_key= 'batter_id'
    )
}}

WITH cte_plate_appearance AS (
  SELECT * FROM {{ref('stg_plate_appearance')}}
  ),
cte_game_count AS (
  SELECT * FROM {{ref('stg_game_count')}}
  ),
cte_strikeouts AS (
  SELECT
     DISTINCT batter_id
    ,batter_full_name
    ,gm_date
    ,game_year
    ,SUM(strikeout) OVER (PARTITION BY batter_id, game_year ORDER BY gm_date) AS running_count
  FROM cte_plate_appearance
  ),
cte_first_to_hundred AS (
  SELECT
    batter_id
   ,batter_full_name
   ,game_year
   ,MIN(gm_date) AS date_to_100_k
  FROM cte_strikeouts
  WHERE running_count = 100
  GROUP BY 1,2,3
  ORDER BY 4 DESC
  ),
cte_final AS (
  SELECT * FROM cte_first_to_hundred
  )

SELECT * FROM cte_final
ORDER BY 4