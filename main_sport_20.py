import psycopg2
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2
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

    player_stats_query = """
        CREATE TABLE IF NOT EXISTS player_stats (
            id SERIAL PRIMARY KEY,
            GAME_ID VARCHAR,
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR,
            TEAM_CITY VARCHAR,
            PLAYER_ID INT,
            PLAYER_NAME VARCHAR,
            NICKNAME VARCHAR,
            START_POSITION VARCHAR,
            COMMENT VARCHAR,
            MIN VARCHAR,
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
            PTS INT,
            PLUS_MINUS INT
        )
    """
    cursor.execute(player_stats_query)

    team_starter_bench_stats_query = """
        CREATE TABLE IF NOT EXISTS team_starter_bench_stats (
            id SERIAL PRIMARY KEY,
            GAME_ID VARCHAR,
            TEAM_ID int,
            TEAM_NAME VARCHAR,
            TEAM_ABBREVIATION VARCHAR,
            TEAM_CITY VARCHAR,
            STARTERS_BENCH VARCHAR,
            MIN VARCHAR,
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
            PTS INT
        )
    """
    cursor.execute(team_starter_bench_stats_query)

    team_stats_query = """
        CREATE TABLE IF NOT EXISTS team_stats (
            id SERIAL PRIMARY KEY,
            GAME_ID VARCHAR,
            TEAM_ID INT,
            TEAM_NAME VARCHAR,
            TEAM_ABBREVIATION VARCHAR,
            TEAM_CITY VARCHAR,
            MIN VARCHAR,
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
            PTS INT,
            PLUS_MINUS INT
        )
    """
    cursor.execute(team_stats_query)

    game_ids = []

    for game in all_game_json.data_sets[0].data['data']:
        game_id = game[4]
        if game_id not in game_ids:
            game_ids.append(game_id)
    conn.commit()

    tblNames = {}
    tblNames['PlayerStats'] = 'player_stats'
    tblNames['TeamStats'] = 'team_stats'
    tblNames['TeamStarterBenchStats'] = 'team_starter_bench_stats'

    cursor.execute("BEGIN;")
    cursor.execute('DELETE FROM player_stats')
    cursor.execute('DELETE FROM team_starter_bench_stats')
    cursor.execute('DELETE FROM team_stats')
    for id in game_ids:
        jsonstr = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=id,end_period=1,start_period=1).get_json()
        jsondata = json.loads(jsonstr)
        for result in jsondata['resultSets']:
            if result['name'] in tblNames and len(result['rowSet']) > 0:
                tblName = tblNames[result['name']]
                sql = "INSERT INTO " + tblName + "("
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