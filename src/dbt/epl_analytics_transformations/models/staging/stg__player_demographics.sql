with player_demographics as (

    select
        {{ dbt_utils.generate_surrogate_key(['Player_Names', 'Age']) }} as primary_key,
        Player_Names as player_names,
        Club as club,
        Nationality as nationality,
        Age as age,
        Position as playing_position,
        Prefered_Foot as prefered_playing_foot,
        Height as height,
        Weight as weight,
        Previous_Teams as previous_teams,
        DOB as date_of_birth,
        Season as season

    from {{ source('epl_analytics_dwh', 'player_demo_data') }}
)

select * from player_demographics
