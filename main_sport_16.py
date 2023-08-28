import psycopg2
from nba_api.stats.endpoints import scoreboardv2
import json
from datetime import date, timedelta

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

    line_score_query = """
        CREATE TABLE IF NOT EXISTS line_score (
            id SERIAL PRIMARY KEY,
            GAME_DATE_EST varchar,
            GAME_SEQUENCE int,
            GAME_ID varchar,
            TEAM_ID int,
            TEAM_ABBREVIATION varchar,
            TEAM_CITY_NAME varchar,
            TEAM_NAME varchar,
            TEAM_WINS_LOSSES varchar,
            PTS_QTR1 int,
            PTS_QTR2 int,
            PTS_QTR3 int,
            PTS_QTR4 int,
            PTS_OT1 int,
            PTS_OT2 int,
            PTS_OT3 int,
            PTS_OT4 int,
            PTS_OT5 int,
            PTS_OT6 int,
            PTS_OT7 int,
            PTS_OT8 int,
            PTS_OT9 int,
            PTS_OT10 int,
            PTS int,
            FG_PCT float4,
            FT_PCT float4,
            FG3_PCT float4,
            AST int,
            REB int,
            TOV int
        )
    """
    cursor.execute(line_score_query)

    cursor.execute('DELETE FROM line_score')

    today = date.today()
    formatted_date = today.strftime("%Y-%m-%d")
    no_data = False
    jsonstr = ""
    jsondata = {}

    try:
        jsonstr = scoreboardv2.ScoreboardV2(day_offset=1,league_id=0,game_date=formatted_date).get_json()
        jsondata = json.loads(jsonstr)
    except Exception as e:
        no_data = True

    if no_data:
        yesterday = today - timedelta(days=1)
        formatted_date = yesterday.strftime("%Y-%m-%d")
        jsonstr = scoreboardv2.ScoreboardV2(day_offset=1,league_id=0,game_date=formatted_date).get_json()
        jsondata = json.loads(jsonstr)

    for result in jsondata['resultSets']:
        if result['name'] == "LineScore":
            sql = "INSERT INTO line_score("
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
            break

    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    # Code to handle any other exceptions
    print("An error occurred:", str(e))