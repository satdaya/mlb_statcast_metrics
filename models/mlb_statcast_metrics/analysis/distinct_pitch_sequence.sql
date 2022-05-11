{{
    config(
      materialized= 'table',
      unique_key= 'dist_pitch_seq'
    )
}}

with _pitch_sequence as (
  select * from {{ref('pitch_sequences')}}
),
_distinct_pitch_sequence as (
  select
     pitch_sequence as dist_pitch_seq
    ,count (dist_pitch_seq) as sum_sequence_used
    ,case when sum(is_at_bat_bool) = 0
          then null
          else round( sum(ab_safe_or_out_bool) / sum(is_at_bat_bool) , 3) 
          end as batting_avg
    ,case when sum(is_plate_appearance_bool) = 0
          then null
          else round( sum(pa_safe_or_out_bool) / sum(is_plate_appearance_bool), 3)
          end as obp --on base percentage
    ,case when sum(is_at_bat_bool) = 0
          then null
          else round( sum(bases_for_slg) / sum(is_at_bat_bool), 3)
          end as slg_percentage
    ,obp + slg_percentage as ops
   from _pitch_sequence
  group by 1
  order by 2 desc
  ),
_final as ( 
  select * from _distinct_pitch_sequence
)

select * from _final