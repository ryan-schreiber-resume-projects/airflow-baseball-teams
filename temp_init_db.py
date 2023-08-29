
import psycopg2
import pandas
import sqlalchemy
import statsapi
import time

username = "airflow"
password = "airflow"
database = "airflow"
database_url = f"postgresql://{username}:{password}@localhost:5432/{database}"
engine = sqlalchemy.create_engine(database_url)

# start by droping all existing tables
with psycopg2.connect(database_url) as connection:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS test_table")
        connection.commit()


# teams reference data
teams = [
  {
    "team_id": "108",
    "code": "ana",
    "file_code": "ana",
    "abbreviation": "LAA",
    "name": "LA Angels",
    "full_name": "Los Angeles Angels",
    "brief_name": "Angels"
  },
  {
    "team_id": "109",
    "code": "ari",
    "file_code": "ari",
    "abbreviation": "ARI",
    "name": "Arizona",
    "full_name": "Arizona Diamondbacks",
    "brief_name": "D-backs"
  },
  {
    "team_id": "110",
    "code": "bal",
    "file_code": "bal",
    "abbreviation": "BAL",
    "name": "Baltimore",
    "full_name": "Baltimore Orioles",
    "brief_name": "Orioles"
  },
  {
    "team_id": "111",
    "code": "bos",
    "file_code": "bos",
    "abbreviation": "BOS",
    "name": "Boston",
    "full_name": "Boston Red Sox",
    "brief_name": "Red Sox"
  },
  {
    "team_id": "112",
    "code": "chn",
    "file_code": "chc",
    "abbreviation": "CHC",
    "name": "Chi Cubs",
    "full_name": "Chicago Cubs",
    "brief_name": "Cubs"
  },
  {
    "team_id": "113",
    "code": "cin",
    "file_code": "cin",
    "abbreviation": "CIN",
    "name": "Cincinnati",
    "full_name": "Cincinnati Reds",
    "brief_name": "Reds"
  },
  {
    "team_id": "114",
    "code": "cle",
    "file_code": "cle",
    "abbreviation": "CLE",
    "name": "Cleveland",
    "full_name": "Cleveland Guardians",
    "brief_name": "Guardians"
  },
  {
    "team_id": "115",
    "code": "col",
    "file_code": "col",
    "abbreviation": "COL",
    "name": "Colorado",
    "full_name": "Colorado Rockies",
    "brief_name": "Rockies"
  },
  {
    "team_id": "116",
    "code": "det",
    "file_code": "det",
    "abbreviation": "DET",
    "name": "Detroit",
    "full_name": "Detroit Tigers",
    "brief_name": "Tigers"
  },
  {
    "team_id": "117",
    "code": "hou",
    "file_code": "hou",
    "abbreviation": "HOU",
    "name": "Houston",
    "full_name": "Houston Astros",
    "brief_name": "Astros"
  },
  {
    "team_id": "118",
    "code": "kca",
    "file_code": "kc",
    "abbreviation": "KC",
    "name": "Kansas City",
    "full_name": "Kansas City Royals",
    "brief_name": "Royals"
  },
  {
    "team_id": "119",
    "code": "lan",
    "file_code": "la",
    "abbreviation": "LAD",
    "name": "LA Dodgers",
    "full_name": "Los Angeles Dodgers",
    "brief_name": "Dodgers"
  },
  {
    "team_id": "120",
    "code": "was",
    "file_code": "was",
    "abbreviation": "WSH",
    "name": "Washington",
    "full_name": "Washington Nationals",
    "brief_name": "Nationals"
  },
  {
    "team_id": "121",
    "code": "nyn",
    "file_code": "nym",
    "abbreviation": "NYM",
    "name": "NY Mets",
    "full_name": "New York Mets",
    "brief_name": "Mets"
  },
  {
    "team_id": "133",
    "code": "oak",
    "file_code": "oak",
    "abbreviation": "OAK",
    "name": "Oakland",
    "full_name": "Oakland Athletics",
    "brief_name": "Athletics"
  },
  {
    "team_id": "134",
    "code": "pit",
    "file_code": "pit",
    "abbreviation": "PIT",
    "name": "Pittsburgh",
    "full_name": "Pittsburgh Pirates",
    "brief_name": "Pirates"
  },
  {
    "team_id": "135",
    "code": "sdn",
    "file_code": "sd",
    "abbreviation": "SD",
    "name": "San Diego",
    "full_name": "San Diego Padres",
    "brief_name": "Padres"
  },
  {
    "team_id": "136",
    "code": "sea",
    "file_code": "sea",
    "abbreviation": "SEA",
    "name": "Seattle",
    "full_name": "Seattle Mariners",
    "brief_name": "Mariners"
  },
  {
    "team_id": "137",
    "code": "sfn",
    "file_code": "sf",
    "abbreviation": "SF",
    "name": "San Francisco",
    "full_name": "San Francisco Giants",
    "brief_name": "Giants"
  },
  {
    "team_id": "138",
    "code": "sln",
    "file_code": "stl",
    "abbreviation": "STL",
    "name": "St. Louis",
    "full_name": "St. Louis Cardinals",
    "brief_name": "Cardinals"
  },
  {
    "team_id": "139",
    "code": "tba",
    "file_code": "tb",
    "abbreviation": "TB",
    "name": "Tampa Bay",
    "full_name": "Tampa Bay Rays",
    "brief_name": "Rays"
  },
  {
    "team_id": "140",
    "code": "tex",
    "file_code": "tex",
    "abbreviation": "TEX",
    "name": "Texas",
    "full_name": "Texas Rangers",
    "brief_name": "Rangers"
  },
  {
    "team_id": "141",
    "code": "tor",
    "file_code": "tor",
    "abbreviation": "TOR",
    "name": "Toronto",
    "full_name": "Toronto Blue Jays",
    "brief_name": "Blue Jays"
  },
  {
    "team_id": "142",
    "code": "min",
    "file_code": "min",
    "abbreviation": "MIN",
    "name": "Minnesota",
    "full_name": "Minnesota Twins",
    "brief_name": "Twins"
  },
  {
    "team_id": "143",
    "code": "phi",
    "file_code": "phi",
    "abbreviation": "PHI",
    "name": "Philadelphia",
    "full_name": "Philadelphia Phillies",
    "brief_name": "Phillies"
  },
  {
    "team_id": "144",
    "code": "atl",
    "file_code": "atl",
    "abbreviation": "ATL",
    "name": "Atlanta",
    "full_name": "Atlanta Braves",
    "brief_name": "Braves"
  },
  {
    "team_id": "145",
    "code": "cha",
    "file_code": "cws",
    "abbreviation": "CWS",
    "name": "Chi White Sox",
    "full_name": "Chicago White Sox",
    "brief_name": "White Sox"
  },
  {
    "team_id": "146",
    "code": "mia",
    "file_code": "mia",
    "abbreviation": "MIA",
    "name": "Miami",
    "full_name": "Miami Marlins",
    "brief_name": "Marlins"
  },
  {
    "team_id": "147",
    "code": "nya",
    "file_code": "nyy",
    "abbreviation": "NYY",
    "name": "NY Yankees",
    "full_name": "New York Yankees",
    "brief_name": "Yankees"
  },
  {
    "team_id": "158",
    "code": "mil",
    "file_code": "mil",
    "abbreviation": "MIL",
    "name": "Milwaukee",
    "full_name": "Milwaukee Brewers",
    "brief_name": "Brewers"
  },
  {
    "team_id": "159",
    "code": "aas",
    "file_code": "al",
    "abbreviation": "AL",
    "name": "AL All-Stars",
    "full_name": "American League All-Stars",
    "brief_name": "AL All-Stars"
  },
  {
    "team_id": "160",
    "code": "nas",
    "file_code": "nl",
    "abbreviation": "NL",
    "name": "NL All-Stars",
    "full_name": "National League All-Stars",
    "brief_name": "NL All-Stars"
  }
]
df = pandas.DataFrame(teams)
df.to_sql('teams', engine, if_exists='replace', index=False)

# collect the raw data from the api
start_date = '01/01/2022'
end_date = '12/15/2022'
team_ids = pandas.read_sql("""SELECT team_id FROM teams LIMIT 30""", engine)["team_id"]

df = None
for team_id in team_ids:
    print(f"Gathering team id: {team_id}")
    temp = pandas.DataFrame(statsapi.schedule(
        team=team_id,
        start_date=start_date,
        end_date=end_date,
    ))
    if df is None:
        df = temp
    else:
        df = pandas.concat([df, temp]).drop_duplicates(["game_id"])
    time.sleep(2)

df.to_sql('raw', engine, if_exists='replace', index=False)



raw = pandas.read_sql("""SELECT * FROM raw LIMIT 1""", engine)



