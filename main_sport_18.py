import psycopg2
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.library import parameters
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
    
    create_common_team_roster_query = """
        CREATE TABLE IF NOT EXISTS common_team_roster (
            id SERIAL PRIMARY KEY,
            TEAMID INT,
            SEASON VARCHAR,
            LEAGUEID VARCHAR,
            PLAYER VARCHAR,
            NICKNAME VARCHAR,
            PLAYER_SLUG VARCHAR,
            NUM VARCHAR,
            POSITION VARCHAR,
            HEIGHT VARCHAR,
            WEIGHT VARCHAR,
            BIRTH_DATE VARCHAR,
            AGE INT,
            EXP VARCHAR,
            SCHOOL VARCHAR,
            PLAYER_ID INT,
            HOW_ACQUIRED VARCHAR
        )
    """
    cursor.execute(create_common_team_roster_query)
    cursor.execute("DELETE FROM common_team_roster")

    all_teams = teams.get_teams()

    for team in all_teams:
        jsonstr = ""
        jsondata = {}
        try:
            jsonstr = commonteamroster.CommonTeamRoster(season='2023-24',team_id=team.get('id')).get_json()
            jsondata = json.loads(jsonstr)
        except Exception as e:
            break            
        for result in jsondata['resultSets']:
            if result['name'] == "CommonTeamRoster":
                sql = "INSERT INTO common_team_roster("
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