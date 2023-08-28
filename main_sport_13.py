import psycopg2
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoresummaryv2
import json

try:

    # Get all games.
    all_game_json = leaguegamefinder.LeagueGameFinder()

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="scraper",
        user="postgres",
        password="!QAZxsw2"
    )

    conn.autocommit = False
    cursor = conn.cursor()
    cursor.execute("BEGIN;")

    create_game_info_query = """
        CREATE TABLE IF NOT EXISTS game_info (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR,
            game_date VARCHAR
        )
    """
    
    cursor.execute(create_game_info_query)

    game_ids = []

    cursor.execute('DELETE FROM game_info')

    for game in all_game_json.data_sets[0].data['data']:
        game_id = game[4]
        if game_id not in game_ids:
            game_ids.append(game_id)
            insert_query = """
                INSERT INTO game_info (game_id, game_date)
                VALUES (%s, %s)
            """
            game_date = game[5]
            data = (game_id, game_date)
            cursor.execute(insert_query, data)
    conn.commit()

    cursor.close()
    conn.close()

except Exception as e:
    # Code to handle any other exceptions
    print("An error occurred:", str(e))