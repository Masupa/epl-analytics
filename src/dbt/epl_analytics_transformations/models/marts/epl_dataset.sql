with
	player_demographics as (
		select * from {{ ref('stg__player_demographics') }}
	),
	
	player_stats as (
		select * from {{ ref('player_game_stats') }}
	),
	
	final as (
		select
			demo.player_names,
			demo.club,
			demo.nationality,
			cast(demo.age as int64) as age,
			demo.playing_position,
			demo.prefered_playing_foot,
			demo.height,
			demo.weight,
			demo.previous_teams,
			demo.date_of_birth,
			stats.season,
			stats.goals_scored,
			stats.assists_made,
			stats.yellow_cards_received,
			stats.red_cards_received
		
		from player_demographics as demo
		left join player_stats as stats
			on demo.player_names = stats.player_names
	)
	

select * from final
