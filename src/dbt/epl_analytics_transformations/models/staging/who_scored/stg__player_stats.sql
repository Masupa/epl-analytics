{{ 
    config(cluster_by=['Season']) 
}}

with player_stats as (
    select
        {{ dbt_utils.generate_surrogate_key(['Player', 'Season']) }} as player_stats_id,
        Player as player_names,
        cast(Mins as int64) as minutes_played,
        cast(Goals as int64) as goals_scored,
        cast(Assists as int64) as assists_made,
        cast(Yel as int64) as yellow_cards_received,
        cast(Red as int64) as red_cards_received,
        cast(SpG as float64) as shots_per_game,
        cast(PS_ as float64) as pass_success_percentage,
        cast(AerialsWon as float64) as aerial_duels_won_per_game,
        cast(MotM as int64) as man_of_the_match,
        Season as season
    
    from {{ source('epl_analytics_dwh', 'player_stats_data') }}
)

select * from player_stats
