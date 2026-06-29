from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum
import json

# Competition code for UEFA Champions League
# Change this to 'PL', 'BL1', 'SA' etc. to switch competitions
COMPETITION_CODE = 'CL'

# Airflow connection ID for Neon Postgres database
POSTGRES_CONN_ID = 'postgres_default'

# Airflow connection ID for football-data.org API
# Connection stores the base URL and X-Auth-Token header
API_CONN_ID = 'football_api'

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1)
}

# DAG definition
# dag_id: unique name shown in the Airflow UI
# schedule: how often the DAG runs (@daily = once per day)
# catchup=False: don't backfill missed runs if Airflow was offline
with DAG(
    dag_id='cl_football_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='ETL pipeline for UEFA Champions League data'
) as dag:

    @task()
    def extract_teams():
        """Extract teams data from football-data.org API."""

        # HttpHook uses the football_api connection from Airflow UI
        # which already stores the base URL and API key as a header
        # so we don't need to hardcode any credentials here
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Full URL becomes: https://api.football-data.org/v4/competitions/CL/teams
        endpoint = f'/v4/competitions/{COMPETITION_CODE}/teams'

        # Make the API request — HttpHook handles attaching the auth header
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            # Convert the raw JSON response into a Python dictionary
            # and pass it to the next task (transform_teams)
            return response.json()
        else:
            # Stop the task and mark it as failed in the Airflow UI
            raise Exception(f"Failed to fetch teams data: {response.status_code}")

    @task()
    def transform_teams(teams_data):
        """Transform raw teams data into a list of records for loading."""

        # Start with an empty list — we'll append one dictionary per team
        teams = []

        # The API wraps the team list under a 'teams' key
        # teams_data['teams'] gives us the actual list of team objects
        for team in teams_data['teams']:

            # For each team, extract only the fields we need
            # and append a clean dictionary to our list
            # This filters out fields we don't need (website, emblem, address etc.)
            teams.append({
                'team_id': team['id'],
                'team_name': team['name'],
                'short_name': team['shortName'],
                'tla': team['tla']  # Three letter abbreviation e.g. 'RMA' for Real Madrid
            })

        # Return the full list of team dictionaries to load_teams
        return teams

    @task()
    def load_teams(transformed_teams):
        """Load transformed teams data into cl_teams table in Neon."""

        # PostgresHook uses the postgres_default connection from Airflow UI
        # which stores our Neon host, database, username, password and sslmode
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # get_conn() opens a live connection to the Neon database
        conn = pg_hook.get_conn()

        # cursor() lets us execute SQL statements
        cursor = conn.cursor()

        # Loop through each team and insert it into cl_teams
        for team in transformed_teams:
            cursor.execute("""
                INSERT INTO cl_teams (team_id, team_name, short_name, tla)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    short_name = EXCLUDED.short_name,
                    tla = EXCLUDED.tla,
                    loaded_at = CURRENT_TIMESTAMP
            """, (
                # %s placeholders are filled in order by this tuple
                # This prevents SQL injection attacks
                team['team_id'],
                team['team_name'],
                team['short_name'],
                team['tla']
            ))
            # ON CONFLICT: if a team with this team_id already exists,
            # update its fields instead of failing with a duplicate key error
            # EXCLUDED refers to the values we just tried to insert

        # commit() saves all the inserts to the database permanently
        # without this, the changes would be lost
        conn.commit()

        # Always close the cursor to free up database resources
        cursor.close()

    @task()
    def extract_standings():
        """Extract standings data from football-data.org API."""

        # Same HttpHook pattern as extract_teams
        # only the endpoint changes
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Full URL becomes: https://api.football-data.org/v4/competitions/CL/standings
        endpoint = f'/v4/competitions/{COMPETITION_CODE}/standings'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch standings data: {response.status_code}")

    @task()
    def transform_standings(standings_data):
        """Transform raw standings data into a list of records for loading."""

        standings = []

        # Extract just the year from the season start date string e.g. "2023-09-19" → "2023"
        # [:4] slices the first 4 characters of the string
        season = standings_data['season']['startDate'][:4]

        # Champions League has multiple groups (A, B, C etc.)
        # standings_data['standings'] is a list of groups
        # Each group has its own table of 4 teams
        for standing in standings_data['standings']:

            # stage e.g. 'GROUP_STAGE', 'ROUND_OF_16' etc.
            stage = standing['stage']

            # group e.g. 'GROUP_A', 'GROUP_B' etc.
            # Will be None for knockout stages which have no groups
            group = standing['group']

            # standing['table'] is the list of team rows within this group
            for entry in standing['table']:

                # entry['team'] is a nested object containing team details
                # entry['team']['id'] links back to cl_teams via the foreign key
                standings.append({
                    'team_id': entry['team']['id'],
                    'position': entry['position'],
                    'played_games': entry['playedGames'],
                    'won': entry['won'],
                    'draw': entry['draw'],
                    'lost': entry['lost'],
                    'points': entry['points'],
                    'goals_for': entry['goalsFor'],
                    'goals_against': entry['goalsAgainst'],
                    'goal_difference': entry['goalDifference'],
                    'stage': stage,
                    'group_name': group,
                    'season': season
                })

        return standings

    @task()
    def load_standings(transformed_standings):
        """Load transformed standings data into cl_standings table in Neon."""

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Full refresh strategy: delete all existing rows first
        # then insert fresh data from the API
        # This ensures standings always reflect the current state
        # rather than mixing old and new values after each matchday
        cursor.execute("DELETE FROM cl_standings;")

        for standing in transformed_standings:
            cursor.execute("""
                INSERT INTO cl_standings (
                    team_id, position, played_games, won, draw, lost,
                    points, goals_for, goals_against, goal_difference,
                    stage, group_name, season
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                standing['team_id'],
                standing['position'],
                standing['played_games'],
                standing['won'],
                standing['draw'],
                standing['lost'],
                standing['points'],
                standing['goals_for'],
                standing['goals_against'],
                standing['goal_difference'],
                standing['stage'],
                standing['group_name'],
                standing['season']
            ))

        conn.commit()
        cursor.close()

    @task()
    def extract_matches():
        """Extract matches data from football-data.org API."""

        # Same HttpHook pattern as the other extract tasks
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Full URL becomes: https://api.football-data.org/v4/competitions/CL/matches
        endpoint = f'/v4/competitions/{COMPETITION_CODE}/matches'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch matches data: {response.status_code}")

    @task()
    def transform_matches(matches_data):
        """Transform raw matches data into a list of records for loading."""

        matches = []

        # matches_data['matches'] is the list of all CL matches for the season
        for match in matches_data['matches']:

            # Scores default to None for scheduled/postponed matches
            # that haven't been played yet — None becomes NULL in Postgres
            home_score = None
            away_score = None
            winner = None

            # Only populate scores if the match has been played
            # The API returns null for fullTime scores on unplayed matches
            if match['score']['fullTime']['home'] is not None:
                home_score = match['score']['fullTime']['home']
                away_score = match['score']['fullTime']['away']
                # winner is 'HOME_TEAM', 'AWAY_TEAM', 'DRAW', or None
                winner = match['score']['winner']

            matches.append({
                'match_id': match['id'],       # Stable unique ID from the API
                'utc_date': match['utcDate'],  # Match date/time in UTC
                'status': match['status'],     # SCHEDULED, FINISHED, POSTPONED etc.
                # .get() safely returns None if the key doesn't exist
                # matchday and group are None for knockout stage matches
                'matchday': match.get('matchday'),
                'stage': match['stage'],
                'group_name': match.get('group'),
                'home_team_id': match['homeTeam']['id'],
                'home_team_name': match['homeTeam']['name'],
                'away_team_id': match['awayTeam']['id'],
                'away_team_name': match['awayTeam']['name'],
                'home_score': home_score,
                'away_score': away_score,
                'winner': winner
            })

        return matches

    @task()
    def load_matches(transformed_matches):
        """Load transformed matches data into cl_matches table in Neon."""

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for match in transformed_matches:
            cursor.execute("""
                INSERT INTO cl_matches (
                    match_id, utc_date, status, matchday, stage, group_name,
                    home_team_id, home_team_name, away_team_id, away_team_name,
                    home_score, away_score, winner
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    winner = EXCLUDED.winner,
                    loaded_at = CURRENT_TIMESTAMP
            """, (
                match['match_id'],
                match['utc_date'],
                match['status'],
                match['matchday'],
                match['stage'],
                match['group_name'],
                match['home_team_id'],
                match['home_team_name'],
                match['away_team_id'],
                match['away_team_name'],
                match['home_score'],
                match['away_score'],
                match['winner']
            ))
            # ON CONFLICT: if this match_id already exists (DAG rerun),
            # update the status and scores instead of inserting a duplicate
            # This handles the case where a scheduled match gets played
            # and we need to fill in the final score

        conn.commit()
        cursor.close()

    ## DAG Workflow - Task dependencies
    # Step 1: Extract and load teams first
    # Teams must exist in cl_teams before standings and matches
    # can reference them via foreign keys
    teams_data = extract_teams()
    transformed_teams = transform_teams(teams_data)
    teams_loaded = load_teams(transformed_teams)

    # Step 2: standings and matches run in parallel after teams are loaded
    # Each follows its own extract → transform → load chain
    standings_data = extract_standings()
    transformed_standings = transform_standings(standings_data)
    load_standings(transformed_standings)

    matches_data = extract_matches()
    transformed_matches = transform_matches(matches_data)
    load_matches(transformed_matches)

    # >> operator means "must run before"
    # [standings_data, matches_data] means both start at the same time
    # So: load_teams finishes → extract_standings and extract_matches start together
    teams_loaded >> [standings_data, matches_data]