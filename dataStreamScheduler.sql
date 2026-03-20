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

CREATE OR REPLACE SECRET riot_api_key
  TYPE = GENERIC_STRING
  SECRET_STRING = 'nope';
  
CREATE OR REPLACE NETWORK RULE RIOT_NETWORK_RULE
    MODE       = EGRESS
    TYPE       = HOST_PORT
    VALUE_LIST = (
        'americas.api.riotgames.com',
        'europe.api.riotgames.com',
        'asia.api.riotgames.com',
        'sea.api.riotgames.com',
        'na1.api.riotgames.com',
        'euw1.api.riotgames.com',
        'kr.api.riotgames.com'
    );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION RIOT_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (RIOT_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (riot_api_key)
    ENABLED = TRUE;

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

GRANT USAGE ON INTEGRATION RIOT_ACCESS_INTEGRATION TO ROLE ACCOUNTADMIN;

INSERT INTO RANKEDDATA.LOLSCHEMA.Players (PLAYERID, TAGLINE, REGION, PLATFORM)
VALUES
    ('sacredswords15', 'NA1',   'americas', 'na1'),
    ('TFBlade',        '122',   'americas', 'na1'),
    ('Solarbacca',     'NA1',   'americas', 'na1'),
    ('Davemon',        'NA1',   'americas', 'na1'),
    ('Annie Bot',      'Tibrs', 'americas', 'na1'),
    ('101100100',      'NA1',   'americas', 'na1'),
    ('FrostPrincess',  'Tiara', 'americas', 'na1');

CREATE OR REPLACE PROCEDURE RANKEDDATA.LOLSCHEMA.COLLECT_ALL_PLAYERS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (RIOT_ACCESS_INTEGRATION)
SECRETS = ('riot_api_key' = RANKEDDATA.LOLSCHEMA.riot_api_key)
HANDLER = 'collect_all_players'
EXECUTE AS CALLER
AS
$$
import requests
import _snowflake
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

def collect_all_players(session):
    # Fetch the API key from the Snowflake secret at runtime
    api_key = _snowflake.get_generic_secret_string('riot_api_key')

    players = session.sql(
        "SELECT PLAYERID, TAGLINE, REGION, PLATFORM FROM RANKEDDATA.LOLSCHEMA.Players"
    ).collect()

    results = []
    for row in players:
        summoner_name = row["PLAYERID"]
        tagline       = row["TAGLINE"]
        region        = row["REGION"]
        platform      = row["PLATFORM"]
        try:
            puuid = get_puuid(summoner_name, tagline, region, api_key)
            rank  = get_current_rank(puuid, platform, api_key)

            if rank is None:
                results.append(f"SKIPPED {summoner_name}#{tagline} — Unranked")
                continue

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

            results.append(f"OK {summoner_name}#{tagline} — {rank} ({numeric_rank} pts)")

        except Exception as e:
            results.append(f"ERROR {summoner_name}#{tagline}: {str(e)}")

    return "\n".join(results)
$$;

-- Test immediately and see the full output
CALL RANKEDDATA.LOLSCHEMA.COLLECT_ALL_PLAYERS();

ALTER TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION SUSPEND;

CREATE OR REPLACE TASK RANKEDDATA.LOLSCHEMA.DAILY_RANK_COLLECTION
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 8 * * * America/New_York'
AS
    CALL RANKEDDATA.LOLSCHEMA.COLLECT_ALL_PLAYERS();

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

-- Check the output/return value of the last task run
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME    => 'DAILY_RANK_COLLECTION',
    RESULT_LIMIT => 10
))
ORDER BY SCHEDULED_TIME DESC;

-- See exactly what was inserted today
SELECT * FROM RANKEDDATA.LOLSCHEMA.VisualsTable
WHERE DATE = CURRENT_DATE
ORDER BY PLAYERID;

-- See which players are missing today's entry
SELECT PLAYERID, TAGLINE
FROM RANKEDDATA.LOLSCHEMA.Players
WHERE (PLAYERID, TAGLINE) NOT IN (
    SELECT PLAYERID, TAGLINE
    FROM RANKEDDATA.LOLSCHEMA.VisualsTable
    WHERE DATE = CURRENT_DATE
);

ALTER USER SUYOGKHANAL SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAra+0fQXEVcEYFpuAB2CY
G+u4l9nqBKrhhRtU30hJT+No8uYvLFcNGP3ZV8H5l/ckJcq6b3Ok/tfuD96naxB9
m3PVoYGcdt0aq3EBfMUfU4Ydc8kcWan5WO3aGcbMvhEllGFPId9SguyhkqcqZb4G
kgHmKFm+MpyWhR80wDy0CP14GjHHariGEVz/0Nv+PR6tlrzfv4i3nMNgPNinTjRb
QI8iikgeAAGcgw+FBHdPhrmb0y4+cF22RAwN98RC+aAlMhB/z3KXvbsHbHv7XU9A
Se3pK8x/prwsDPDaSVZt20iK9fNaPtpLmxUwdZ8qOekVB00O4tN3eu5ye+XklhQ1
DwIDAQAB';

DELETE FROM RANKEDDATA.LOLSCHEMA.VisualsTable
WHERE DATE = CURRENT_DATE;

CREATE OR REPLACE PROCEDURE RANKEDDATA.LOLSCHEMA.ADD_PLAYER(
    PLAYERID VARCHAR,
    TAGLINE  VARCHAR,
    REGION   VARCHAR,
    PLATFORM VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'add_player'
EXECUTE AS CALLER
AS
$$
def add_player(session, playerid, tagline, region, platform):
    try:
        existing = session.sql("""
            SELECT COUNT(*) AS CNT
            FROM RANKEDDATA.LOLSCHEMA.Players
            WHERE PLAYERID = ? AND TAGLINE = ? AND REGION = ?
        """, params=[playerid, tagline, region]).collect()

        if existing[0]["CNT"] > 0:
            return f"SKIPPED {playerid}#{tagline} — Player already exists"

        session.sql("""
            INSERT INTO RANKEDDATA.LOLSCHEMA.Players (PLAYERID, TAGLINE, REGION, PLATFORM)
            VALUES (?, ?, ?, ?)
        """, params=[playerid, tagline, region, platform]).collect()

        return f"OK {playerid}#{tagline} added (region={region}, platform={platform})"

    except Exception as e:
        return f"ERROR {playerid}#{tagline}: {str(e)}"
$$;

select * from players;