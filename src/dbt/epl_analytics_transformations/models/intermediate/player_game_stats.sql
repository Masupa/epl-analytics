with player_stats as (
	select
		player_names,
		season,
		sum(goals_scored) as goals_scored,
		sum(assists_made) as assists_made,
		sum(yellow_cards_received) as yellow_cards_received,
		sum(red_cards_received) as red_cards_received
		
	from {{ ref('stg__player_stats') }}
	group by player_names, season
)

select * from player_stats
