RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTIONCREATE TABLE IF NOT EXISTS RANKEDDATA.LOLSCHEMA.VisualsTable (
    DATE          DATE,
    PLAYERID      VARCHAR,
    RANK          NUMBER,
    MASTERY       NUMBER,
    LEVEL         NUMBER,
    TOTALRANKED   NUMBER,
    RANKEDWINS    NUMBER,
    RANKEDWINRATE FLOAT
);

ALTER TABLE RANKEDDATA.LOLSCHEMA.VisualsTable
    ADD COLUMN TAGLINE VARCHAR;

CREATE TABLE IF NOT EXISTS RANKEDDATA.LOLSCHEMA.Players (
    PLAYERID      VARCHAR,
    TAGLINE       VARCHAR,
    REGION        VARCHAR DEFAULT 'americas',
    PLATFORM      VARCHAR DEFAULT 'na1',
    PRIMARY KEY (PLAYERID, TAGLINE)
);

INSERT INTO RANKEDDATA.LOLSCHEMA.Players (PLAYERID, TAGLINE)
    SELECT DISTINCT PLAYERID, TAGLINE
    FROM RANKEDDATA.LOLSCHEMA.VisualsTable
    WHERE TAGLINE IS NOT NULL;

INSERT INTO RANKEDDATA.LOLSCHEMA.Players (PLAYERID, TAGLINE, REGION, PLATFORM)
VALUES
    ('sacredswords15', 'NA1',   'americas', 'na1'),
    ('TFBlade',        '122',   'americas', 'na1'),
    ('Solarbacca',     'NA1',   'americas', 'na1'),
    ('Davemon',        'NA1',   'americas', 'na1'),
    ('Annie Bot',      'Tibrs', 'americas', 'na1'),
    ('101100100',      'NA1',   'americas', 'na1'),
    ('FrostPrincess',  'Tiara', 'americas', 'na1');

CREATE OR REPLACE PROCEDURE RANKEDDATA.LOLSCHEMA.COLLECT_AND_STORE(
    SUMMONER_NAME VARCHAR,
    API_KEY       VARCHAR,
    TAGLINE       VARCHAR DEFAULT 'NA1',
    REGION        VARCHAR DEFAULT 'americas',
    PLATFORM      VARCHAR DEFAULT 'na1'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'collect_and_store'
EXECUTE AS CALLER
AS
$$
import requests
from datetime import datetime

def riot_get(url, api_key):
    r = requests.get(url, headers={"X-Riot-Token": api_key})
    r.raise_for_status()
    return r.json()

def get_puuid(summoner_name, tagline, region, api_key):
    url = f"https://{region}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{summoner_name}/{tagline}"
    return riot_get(url, api_key)["puuid"]

def get_current_rank(puuid, platform, api_key):
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    for entry in riot_get(url, api_key):
        if entry["queueType"] == "RANKED_SOLO_5x5":
            return f"{entry['tier']} {entry['rank']} - {entry['leaguePoints']} LP"
    return None

def get_ranked_stats(puuid, platform, api_key):
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    for entry in riot_get(url, api_key):
        if entry["queueType"] == "RANKED_SOLO_5x5":
            wins    = entry["wins"]
            losses  = entry["losses"]
            total   = wins + losses
            winrate = round((wins / total) * 100, 1) if total > 0 else 0
            return {"wins": wins, "losses": losses, "total": total, "winrate": winrate}
    return {"wins": 0, "losses": 0, "total": 0, "winrate": 0}

def get_champion_mastery_total(puuid, platform, api_key):
    url = f"https://{platform}.api.riotgames.com/lol/champion-mastery/v4/scores/by-puuid/{puuid}"
    return riot_get(url, api_key)

def get_summoner_level(puuid, platform, api_key):
    url = f"https://{platform}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    return riot_get(url, api_key)["summonerLevel"]

def convert_rank_to_numeric(rank_str):
    tier, division, _, lp, _ = rank_str.split()
    tier_value = {
        "IRON": 0, "BRONZE": 400, "SILVER": 800, "GOLD": 1200,
        "PLATINUM": 1600, "EMERALD": 2000, "DIAMOND": 2400,
        "MASTER": 2800, "GRANDMASTER": 2800, "CHALLENGER": 2800
    }.get(tier, -1)
    if tier_value == -1:
        raise ValueError(f"Invalid rank string: {rank_str}")
    division_value = {"IV": 0, "III": 100, "II": 200, "I": 300}.get(division, 0)
    return tier_value + division_value + int(lp)

def collect_and_store(session, summoner_name, api_key, tagline="NA1", region="americas", platform="na1"):
    try:
        puuid = get_puuid(summoner_name, tagline, region, api_key)
        rank  = get_current_rank(puuid, platform, api_key)

        if rank is None:
            return f"{summoner_name}#{tagline} is Unranked — skipped insert."

        numeric_rank = convert_rank_to_numeric(rank)
        mastery      = get_champion_mastery_total(puuid, platform, api_key)
        level        = get_summoner_level(puuid, platform, api_key)
        stats        = get_ranked_stats(puuid, platform, api_key)
        today        = datetime.now().date()

        session.sql("""
            INSERT INTO RANKEDDATA.LOLSCHEMA.VisualsTable
                (DATE, PLAYERID, TAGLINE, RANK, MASTERY, LEVEL, TOTALRANKED, RANKEDWINS, RANKEDWINRATE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, params=[
            today, summoner_name, tagline, numeric_rank, mastery,
            level, stats["total"], stats["wins"], stats["winrate"]
        ]).collect()

        return f"Inserted {summoner_name}#{tagline} for {today} — {rank} ({numeric_rank} pts)"

    except Exception as e:
        return f"ERROR for {summoner_name}#{tagline}: {str(e)}"
$$;

CREATE OR REPLACE PROCEDURE RANKEDDATA.LOLSCHEMA.COLLECT_ALL_PLAYERS(
    API_KEY VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'collect_all_players'
EXECUTE AS CALLER
AS
$$
def collect_all_players(session, api_key):
    players = session.sql(
        "SELECT PLAYERID, TAGLINE, REGION, PLATFORM FROM RANKEDDATA.LOLSCHEMA.Players"
    ).collect()

    results = []
    for row in players:
        result = session.sql("""
            CALL RANKEDDATA.LOLSCHEMA.COLLECT_AND_STORE(?, ?, ?, ?, ?)
        """, params=[
            row["PLAYERID"], api_key, row["TAGLINE"], row["REGION"], row["PLATFORM"]
        ]).collect()
        results.append(result[0][0])

    return "\n".join(results)
$$;

ALTER TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION SUSPEND;

CREATE OR REPLACE TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 8 * * * America/New_York'
AS
    CALL RANKEDDATA.LOLSCHEMA.COLLECT_ALL_PLAYERS('');

ALTER TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION RESUME;

EXECUTE TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION;

SHOW TASKS IN SCHEMA RANKEDDATA.LOLSCHEMA;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME    => 'DAILY_RANK_COLLECTION',
    RESULT_LIMIT => 10
))
ORDER BY SCHEDULED_TIME DESC;

EXECUTE TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION;

select * from visualstable;