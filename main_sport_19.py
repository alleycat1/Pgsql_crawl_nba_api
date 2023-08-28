import psycopg2
from nba_api.stats.endpoints import leaguegamefinder
import json

try:
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

    league_game_finder_table_query = """
        CREATE TABLE IF NOT EXISTS league_game_finder (
            id SERIAL PRIMARY KEY,
            SEASON_ID VARCHAR,
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR,
            TEAM_NAME VARCHAR,
            GAME_ID VARCHAR,
            GAME_DATE VARCHAR,
            MATCHUP VARCHAR,
            WL VARCHAR,
            MIN INT,
            PTS INT,
            FGM INT,
            FGA INT,
            FG_PCT FLOAT4,
            FG3M INT,
            FG3A INT,
            FG3_PCT FLOAT4,
            FTM INT,
            FTA INT,
            FT_PCT FLOAT4,
            OREB INT,
            DREB INT,
            REB INT,
            AST INT,
            STL INT,
            BLK INT,
            TOV INT,
            PF INT,
            PLUS_MINUS INT,
            VIDEO_AVAILABLE INT
        )
    """
    
    cursor.execute(league_game_finder_table_query)

    cursor.execute('DELETE FROM league_game_finder')

    jsonstr = leaguegamefinder.LeagueGameFinder().get_json()
    jsondata = json.loads(jsonstr)
    result = jsondata['resultSets'][0]
    
    sql = "INSERT INTO league_game_finder("
    for field in result['headers']:
        if field == 'TO':
            field = 'TOV'
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