import psycopg2
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.library import parameters
import json

try:

    # Get all players.
    all_players = players.get_players()

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

    create_player_table_query = """
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            player_id INT,
            full_name VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            is_active BOOL
        )
    """
    
    cursor.execute(create_player_table_query)

    career_totals_all_star_season_query = """
        CREATE TABLE IF NOT EXISTS career_totals_all_star_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            league_id VARCHAR,
            team_id VARCHAR,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(career_totals_all_star_season_query)

    career_totals_college_season_query = """
        CREATE TABLE IF NOT EXISTS career_totals_college_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            league_id VARCHAR,
            organization_id VARCHAR,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(career_totals_college_season_query)

    career_totals_post_season_query = """
        CREATE TABLE IF NOT EXISTS career_totals_post_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            league_id VARCHAR,
            team_id VARCHAR,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(career_totals_post_season_query)

    career_totals_regular_season_query = """
        CREATE TABLE IF NOT EXISTS career_totals_regular_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            league_id VARCHAR,
            team_id VARCHAR,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(career_totals_regular_season_query)

    season_rankings_post_season_query = """
        CREATE TABLE IF NOT EXISTS season_rankings_post_season (
            id serial PRIMARY KEY,
            player_id INT, 
            season_id VARCHAR, 
            league_id VARCHAR, 
            team_id VARCHAR, 
            team_abbreviation VARCHAR, 
            player_age INT, 
            gp INT, 
            gs INT, 
            rank_min INT, 
            rank_fgm INT, 
            rank_fga INT, 
            rank_fg_pct INT, 
            rank_fg3m INT, 
            rank_fg3a INT, 
            rank_fg3_pct INT, 
            rank_ftm INT, 
            rank_fta INT, 
            rank_ft_pct INT, 
            rank_oreb INT, 
            rank_dreb INT, 
            rank_reb INT, 
            rank_ast INT, 
            rank_stl INT, 
            rank_blk INT, 
            rank_tov INT, 
            rank_pts INT, 
            rank_eff INT
        )
    """
    cursor.execute(season_rankings_post_season_query)

    season_rankings_regular_season_query = """
        CREATE TABLE IF NOT EXISTS season_rankings_regular_season (
            id serial PRIMARY KEY,
            player_id INT, 
            season_id VARCHAR, 
            league_id VARCHAR, 
            team_id VARCHAR, 
            team_abbreviation VARCHAR, 
            player_age INT, 
            gp INT, 
            gs INT, 
            rank_min INT, 
            rank_fgm INT, 
            rank_fga INT, 
            rank_fg_pct INT, 
            rank_fg3m INT, 
            rank_fg3a INT, 
            rank_fg3_pct INT, 
            rank_ftm INT, 
            rank_fta INT, 
            rank_ft_pct INT, 
            rank_oreb INT, 
            rank_dreb INT, 
            rank_reb INT, 
            rank_ast INT, 
            rank_stl INT, 
            rank_blk INT, 
            rank_tov INT, 
            rank_pts INT, 
            rank_eff INT
        )
    """
    cursor.execute(season_rankings_regular_season_query)

    season_totals_all_star_season_query = """
        CREATE TABLE IF NOT EXISTS season_totals_all_star_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            season_id VARCHAR,
            league_id VARCHAR,
            team_id VARCHAR,
            team_abbreviation VARCHAR, 
            player_age INT,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(season_totals_all_star_season_query)

    season_totals_college_season_query = """
        CREATE TABLE IF NOT EXISTS season_totals_college_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            season_id VARCHAR,
            league_id VARCHAR,
            organization_id VARCHAR,
            school_name VARCHAR, 
            player_age INT,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(season_totals_college_season_query)

    season_totals_post_season_query = """
        CREATE TABLE IF NOT EXISTS season_totals_post_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            season_id VARCHAR,
            league_id VARCHAR,
            team_id VARCHAR,
            team_abbreviation VARCHAR, 
            player_age INT,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(season_totals_post_season_query)

    season_totals_regular_season_query = """
        CREATE TABLE IF NOT EXISTS season_totals_regular_season (
            id SERIAL PRIMARY KEY,
            player_id INT,
            season_id VARCHAR,
            league_id VARCHAR,
            team_id VARCHAR,
            team_abbreviation VARCHAR, 
            player_age INT,
            gp INT, 
            gs INT, 
            min FLOAT4, 
            fgm INT, 
            fga INT, 
            fg_pct FLOAT4, 
            fg3m INT, 
            fg3a INT, 
            fg3_pct FLOAT4, 
            ftm INT, 
            fta INT, 
            ft_pct FLOAT4, 
            oreb INT, 
            dreb INT, 
            reb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT
        )
    """
    cursor.execute(season_totals_regular_season_query)

    tblNames = {}
    tblNames['CareerTotalsAllStarSeason'] = 'career_totals_all_star_season'
    tblNames['CareerTotalsCollegeSeason'] = 'career_totals_college_season'
    tblNames['CareerTotalsPostSeason'] = 'career_totals_post_season'
    tblNames['CareerTotalsRegularSeason'] = 'career_totals_regular_season'
    #tblNames['SeasonRankingsPostSeason'] = 'season_rankings_post_season'
    #tblNames['SeasonRankingsRegularSeason'] = 'season_rankings_regular_season'
    #tblNames['SeasonTotalsAllStarSeason'] = 'season_totals_all_star_season'
    #tblNames['SeasonTotalsCollegeSeason'] = 'season_totals_college_season'
    #tblNames['SeasonTotalsPostSeason'] = 'season_totals_post_season'
    #tblNames['SeasonTotalsRegularSeason'] = 'season_totals_regular_season'

    cursor.execute("DELETE FROM players")

    for player in all_players:
        insert_query = """
            INSERT INTO players (player_id, full_name, first_name, last_name, is_active)
            VALUES (%s, %s, %s, %s, %s)
        """
        player_id = player.get('id')
        full_name = player.get('full_name')
        first_name = player.get('first_name')
        last_name = player.get('last_name')
        is_active = player.get('is_active')
        data = (player_id, full_name, first_name, last_name, is_active)
        cursor.execute(insert_query, data)
        #player_career_stats = playercareerstats.PlayerCareerStats(player_id=player_id,league_id_nullable='00',per_mode36='Totals').get_data_frames()
        player_career_stats_json = playercareerstats.PlayerCareerStats(player_id=player_id,league_id_nullable='00',per_mode36='Totals').get_json()
        jsondata = json.loads(player_career_stats_json)
        #career_totals_all_star_season = player_career_stats[0]
        for result in jsondata['resultSets']:
            if result['name'] in tblNames and len(result['rowSet']) > 0:
                tblName = tblNames[result['name']]
                sql = "INSERT INTO " + tblName + "("
                for field in result['headers']:
                    sql += field + ","
                sql = sql[:-1]
                sql += ') VALUES ('
                for i in range(len(result['headers'])):
                    sql += "%s,"
                sql = sql[:-1]
                sql += ')'  
                for row in result['rowSet']:
                    for i in range(len(row)):
                        if row[i] == 'NR':
                            row[i] = 0
                    cursor.execute(sql, row)
                                
    conn.commit()

    cursor.close()
    conn.close()

except Exception as e:
    # Code to handle any other exceptions
    print("An error occurred:", str(e))