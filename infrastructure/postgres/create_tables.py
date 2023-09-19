import os
import psycopg2
from utils import connect


# Commands to create Postgres tables
sql_commands = (
    """
    CREATE TABLE player_demo_data (
        player_names VARCHAR(255),
        club VARCHAR(100),
        nationality VARCHAR(100),
        age INT,
        dob DATE,
        posittion VARCHAR(100),
        preferred_foot VARCHAR(100),
        height VARCHAR(10),
        weight VARCHAR(10),
        previous_teams VARCHAR(255),
        season VARCHAR(20),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE player_stats_data (
        player_names VARCHAR(255),
        mins VARCHAR(100),
        goals VARCHAR(100),
        assists VARCHAR(100),
        yellow_cards VARCHAR(20),
        red_cards VARCHAR(20),
        shots_per_game VARCHAR(100),
        pass_success_percentage VARCHAR (100)
        aerials_onw VARCHAR(50),
        man_of_the_match VARCHAR(20),
        season VARCHAR(20),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    """
)


if __name__ == '__main__':
    # File paths to sql commands
    sql_command_file_names = ['player_data_football_critic']

    for file_name in sql_command_file_names:
        try:
            with open(f"infrastructure/postgres/{file_name}.txt", "r", encoding="utf-8") as file:
                sql_command = file.read()
        except FileNotFoundError as ex:
            # TODO: We want to tell the user that the file is not found
            print(f"{ex.args}: {file_name}.txt")
        except Exception as ex:
            print(f"{ex.args}: {file_name}.txt")
        else:
            # Connect to Postgres DB
            message, conn = connect()

            if message == "okay":
                # Create tables in Postgres DB
                for sql_cmd in sql_commands:
                    cur = conn.cursor()
                    # Execute `string` to create table
                    cur.execute(sql_cmd)
                    # commit the changes
                    cur.commit()
                # close the communication with the PostgreSQL
                conn.close()

                print(conn)

            else:
                # TODO: Think about how to handle this
                pass
