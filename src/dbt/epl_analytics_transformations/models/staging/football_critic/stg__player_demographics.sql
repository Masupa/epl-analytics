{{
    config(
        cluster_by=['club']
    )
}}


with player_demographics as (

    select
        {{ dbt_utils.generate_surrogate_key(['Player_Names', 'Age']) }} as primary_key,
        Player_Names as player_names,
        Club as club,
        Nationality as nationality,
        cast(Age as int64) as age,
        Position as playing_position,
        Prefered_Foot as prefered_playing_foot,
        cast(substring(Height, 0, 3) as int64) as height,
        substring(Height, 5, 2) as height_units,
		cast(substring(Weight, 0, 2) as int64) as weight,
        substring(Weight, 4, 2) weight_units,
        Previous_Teams as previous_teams,
        DOB as date_of_birth,
        Season as season

    from {{ source('epl_analytics_dwh', 'player_demo_data') }}
)

select * from player_demographics
