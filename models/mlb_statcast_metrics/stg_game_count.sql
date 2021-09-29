{{
    config(
      materialized= 'table',
      unique_key= 'comp_key'
    )
}}

WITH cte_home_game AS (
  SELECT
    DISTINCT game_pk AS game_pk
   ,home_team AS team
   ,away_team AS opponent
   ,gm_date
   ,game_year
  FROM base_statcast
  ),
cte_away_game AS (
  SELECT
    DISTINCT game_pk AS game_pk
   ,away_team AS team
   ,home_team AS opponent
   ,gm_date
   ,game_year
  FROM base_statcast
  ),
cte_stack AS (
  SELECT * 
  FROM cte_home_game
  
  UNION ALL
  
  SELECT *
  FROM cte_away_game
  ),
cte_count AS (
  SELECT
    *
   ,COUNT(game_pk) OVER (PARTITION BY team, game_year ORDER BY gm_date) AS game_count
  FROM cte_stack
  )
  
SELECT *
  ,game_pk || team as comp_key
FROM cte_count