{{
    config(
      materialized= 'table',
      unique_key= 'comp_key'
    )
}}

WITH cte_base_statcast AS (
  SELECT * FROM {{ref('base_statcast')}}
),
cte_home_game AS (
  SELECT
    DISTINCT game_pk AS game_pk
   ,home_team AS team
   ,away_team AS opponent
   ,gm_date
   ,game_year
  FROM cte_base_statcast
  ),
cte_away_game AS (
  SELECT
    DISTINCT game_pk AS game_pk
   ,away_team AS team
   ,home_team AS opponent
   ,gm_date
   ,game_year
  FROM cte_base_statcast
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
  ),
cte_yearly_count AS (
  SELECT
    game_year
   ,team
   ,MAX(game_count) AS yearly_game_count
  FROM cte_count
  GROUP BY 1,2
  ),
cte_final AS (
SELECT
   c.game_pk
  ,c.team
  ,c.opponent
  ,c.gm_date
  ,c.game_year
  ,c.game_count
  ,y.yearly_game_count
  ,c.game_pk || c.team as comp_key
FROM cte_count c
JOIN cte_yearly_count y
  ON c.game_year = y.game_year
 AND c.team = y.team
  )
  
SELECT * FROM cte_final