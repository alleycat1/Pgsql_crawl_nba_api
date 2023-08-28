import psycopg2
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2
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

    create_game_id_table_query = """
        CREATE TABLE IF NOT EXISTS game_ids (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR,
            season_id VARCHAR,
            team_id VARCHAR,
            team_abbreviation VARCHAR,
            team_name VARCHAR,
            game_date VARCHAR
        )
    """
    
    cursor.execute(create_game_id_table_query)

    play_by_play_query = """
        CREATE TABLE IF NOT EXISTS play_by_play (
            id SERIAL PRIMARY KEY,
            GAME_ID VARCHAR,
            EVENTNUM VARCHAR,
            EVENTMSGTYPE VARCHAR,
            EVENTMSGACTIONTYPE VARCHAR,
            PERIOD VARCHAR,
            WCTIMESTRING VARCHAR,
            PCTIMESTRING VARCHAR,
            HOMEDESCRIPTION VARCHAR,
            NEUTRALDESCRIPTION VARCHAR,
            VISITORDESCRIPTION VARCHAR,
            SCORE VARCHAR,
            SCOREMARGIN VARCHAR,
            PERSON1TYPE VARCHAR,
            PLAYER1_ID VARCHAR,
            PLAYER1_NAME VARCHAR,
            PLAYER1_TEAM_ID VARCHAR,
            PLAYER1_TEAM_CITY VARCHAR,
            PLAYER1_TEAM_NICKNAME VARCHAR,
            PLAYER1_TEAM_ABBREVIATION VARCHAR,
            PERSON2TYPE VARCHAR,
            PLAYER2_ID VARCHAR,
            PLAYER2_NAME VARCHAR,
            PLAYER2_TEAM_ID VARCHAR,
            PLAYER2_TEAM_CITY VARCHAR,
            PLAYER2_TEAM_NICKNAME VARCHAR,
            PLAYER2_TEAM_ABBREVIATION VARCHAR,
            PERSON3TYPE VARCHAR,
            PLAYER3_ID VARCHAR,
            PLAYER3_NAME VARCHAR,
            PLAYER3_TEAM_ID VARCHAR,
            PLAYER3_TEAM_CITY VARCHAR,
            PLAYER3_TEAM_NICKNAME VARCHAR,
            PLAYER3_TEAM_ABBREVIATION VARCHAR,
            VIDEO_AVAILABLE_FLAG VARCHAR
        )
    """
    cursor.execute(play_by_play_query)

    game_ids = []

    cursor.execute('DELETE FROM game_ids')

    for game in all_game_json.data_sets[0].data['data']:
        if game_id not in game_ids:
            game_ids.append(game_id)
            insert_query = """
                INSERT INTO game_ids (game_id, season_id, team_id, team_abbreviation, team_name, game_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            game_id = game[4]
            season_id = game[0]
            team_id = game[1]
            team_abbreviation = game[2]
            team_name = game[3]
            game_date = game[5]
            data = (game_id, season_id, team_id, team_abbreviation, team_name, game_date)
            cursor.execute(insert_query, data)
    conn.commit()

    cursor.execute("BEGIN;")
    cursor.execute('DELETE FROM play_by_play')
    for id in game_ids:
        jsonstr = playbyplayv2.PlayByPlayV2(game_id=id,end_period=1,start_period=1).get_json()
        jsondata = json.loads(jsonstr)
        result = jsondata['resultSets'][0]

        sql = "INSERT INTO play_by_play("
        for field in result['headers']:
            sql += field + ","
        sql = sql[:-1]
        sql += ') VALUES ('
        for i in range(len(result['headers'])):
            sql += "%s,"
        sql = sql[:-1]
        sql += ')'
        
        for row in result['rowSet']:
            cursor.execute(sql, row)

    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    # Code to handle any other exceptions
    print("An error occurred:", str(e))