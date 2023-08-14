with player_stats as (
    select
        Player as player_names,
        Mins as minutes_played,
        Goals as goals_scored,
        Assists as assists_made,
        Yel as yellow_cards_recieved,
        Red as red_cards_received,
        SpG as shots_per_game,
        PS_ as pass_success_percentage,
        AerialsWon as aerial_duels_won_per_game,
        MotM as man_of_the_match,
        Season as season
    
    from {{ source('epl_analytics_dwh', 'player_stats_data') }}
)

select * from player_stats
