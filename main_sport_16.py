import psycopg2
from nba_api.stats.static import teams
from nba_api.stats.endpoints import teamplayerdashboard
import json

try:
    # Get all teams.
    all_teams = teams.get_teams()

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

    players_season_totals_ranks = """
        CREATE TABLE IF NOT EXISTS players_season_totals (
            id SERIAL PRIMARY KEY,
            GROUP_SET VARCHAR,
            PLAYER_ID INT,
            PLAYER_NAME VARCHAR,
            GP INT,
            W INT,
            L INT,
            W_PCT FLOAT4,
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
            TOV INT,
            STL INT,
            BLK INT,
            BLKA INT,
            PF INT,
            PFD INT,
            PTS INT,
            PLUS_MINUS FLOAT4,
            NBA_FANTASY_PTS FLOAT4,
            DD2 INT,
            TD3 INT,
            GP_RANK INT,
            W_RANK INT,
            L_RANK INT,
            W_PCT_RANK INT,
            MIN_RANK INT,
            FGM_RANK INT,
            FGA_RANK INT,
            FG_PCT_RANK INT,
            FG3M_RANK INT,
            FG3A_RANK INT,
            FG3_PCT_RANK INT,
            FTM_RANK INT,
            FTA_RANK INT,
            FT_PCT_RANK INT,
            OREB_RANK INT,
            DREB_RANK INT,
            REB_RANK INT,
            AST_RANK INT,
            TOV_RANK INT,
            STL_RANK INT,
            BLK_RANK INT,
            BLKA_RANK INT,
            PF_RANK INT,
            PFD_RANK INT,
            PTS_RANK INT,
            PLUS_MINUS_RANK INT,
            NBA_FANTASY_PTS_RANK INT,
            DD2_RANK INT,
            TD3_RANK INT
        )
    """
    cursor.execute(players_season_totals_ranks)

    team_overall_ranks = """
        CREATE TABLE IF NOT EXISTS team_overall (
            id SERIAL PRIMARY KEY,
            GROUP_SET VARCHAR,
            TEAM_ID INT,
            TEAM_NAME VARCHAR,
            GROUP_VALUE VARCHAR,
            GP INT,
            W INT,
            L INT,
            W_PCT  FLOAT4,
            MIN VARCHAR,
            FGM INT,
            FGA INT,
            FG_PCT  FLOAT4,
            FG3M INT,
            FG3A INT,
            FG3_PCT  FLOAT4,
            FTM INT,
            FTA INT,
            FT_PCT  FLOAT4,
            OREB INT,
            DREB INT,
            REB INT,
            AST INT,
            TOV INT,
            STL INT,
            BLK INT,
            BLKA INT,
            PF INT,
            PFD INT,
            PTS INT,
            PLUS_MINUS INT,
            GP_RANK INT,
            W_RANK INT,
            L_RANK INT,
            W_PCT_RANK INT,
            MIN_RANK INT,
            FGM_RANK INT,
            FGA_RANK INT,
            FG_PCT_RANK INT,
            FG3M_RANK INT,
            FG3A_RANK INT,
            FG3_PCT_RANK INT,
            FTM_RANK INT,
            FTA_RANK INT,
            FT_PCT_RANK INT,
            OREB_RANK INT,
            DREB_RANK INT,
            REB_RANK INT,
            AST_RANK INT,
            TOV_RANK INT,
            STL_RANK INT,
            BLK_RANK INT,
            BLKA_RANK INT,
            PF_RANK INT,
            PFD_RANK INT,
            PTS_RANK INT,
            PLUS_MINUS_RANK INT
        )
    """
    cursor.execute(team_overall_ranks)

    tblNames = {}
    tblNames['PlayersSeasonTotals'] = 'players_season_totals'
    tblNames['TeamOverall'] = 'team_overall'
    for team in all_teams:
        team_id = team.get('id')
        try:
            jsonstr = teamplayerdashboard.TeamPlayerDashboard(last_n_games=0,measure_type_detailed_defense='Base',month=0,opponent_team_id=0,pace_adjust='Y',per_mode_detailed='Totals',period=0,plus_minus='N',rank='N',season='2023-24',season_type_all_star='Regular+Season',team_id=team_id).get_json()
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
        except Exception as e:
            print("An error occurred:", str(e))
    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    # Code to handle any other exceptions
    print("An error occurred:", str(e))