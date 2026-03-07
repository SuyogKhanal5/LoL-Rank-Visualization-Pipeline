import os
from dotenv import load_dotenv
import requests
from datetime import datetime
import snowflake.connector as sf

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")

sf_conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    database="RANKEDDATA",
    schema="LOLSCHEMA"
)

headers = {"X-Riot-Token": api_key}

def get_puuid(summoner_name, tagline="NA1", region="americas"):
    url = f"https://{region}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{summoner_name}/{tagline}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["puuid"]  # was missing this

def get_current_rank(puuid, platform="na1"):  # platform not region
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    for entry in r.json():
        if entry["queueType"] == "RANKED_SOLO_5x5":
            return f"{entry['tier']} {entry['rank']} - {entry['leaguePoints']} LP"
    return "Unranked"

def get_champion_mastery_total(puuid, platform="na1"):  # platform not region
    url = f"https://{platform}.api.riotgames.com/lol/champion-mastery/v4/scores/by-puuid/{puuid}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_summoner_level(puuid, platform="na1"):  # platform not region
    url = f"https://{platform}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["summonerLevel"]


def get_ranked_stats(puuid, platform="na1"):  # platform not region
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    for entry in r.json():
        if entry["queueType"] == "RANKED_SOLO_5x5":
            wins = entry["wins"]
            losses = entry["losses"]
            total = wins + losses
            winrate = round((wins / total) * 100, 1) if total > 0 else 0
            return {"wins": wins, "losses": losses, "total": total, "winrate": winrate}    
    return {"wins": 0, "losses": 0, "total": 0, "winrate": 0}

def convert_rank_to_numeric(rank_str):
    tier, division, dummy1, lp, dummy2 = rank_str.split()  # GOLD II - 50 LP
    
    tier_value = {
        "IRON"        : 0,
        "BRONZE"      : 400,
        "SILVER"      : 800,
        "GOLD"        : 1200,
        "PLATINUM"    : 1600,
        "EMERALD"     : 2000,
        "DIAMOND"     : 2400,
        "MASTER"      : 2800,
        "GRANDMASTER" : 2800,
        "CHALLENGER"  : 2800
    }.get(tier, -1)

    division_value = {
        "IV"  : 0,
        "III" : 100,
        "II"  : 200,
        "I"   : 300
    }.get(division, 0)  # 0 not -1, so Master/GM/Challenger don't fail

    lp_value = int(lp)

    if tier_value == -1:
        raise ValueError(f"Invalid rank string: {rank_str}")
    
    return tier_value + division_value + lp_value

def collect_and_store(summoner_name, tagline="NA1"):
    puuid        = get_puuid(summoner_name, tagline)
    date         = datetime.now().date()
    rank         = get_current_rank(puuid)
    numeric_rank = convert_rank_to_numeric(rank)
    mastery      = get_champion_mastery_total(puuid)
    level        = get_summoner_level(puuid)
    stats        = get_ranked_stats(puuid)

    cur = sf_conn.cursor()
    cur.execute("""
        INSERT INTO SACREDSWORDS15
            (DATE, PLAYERID, RANK, MASTERY,
             LEVEL, TOTALRANKED, RANKEDWINS, RANKEDWINRATE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
        date, summoner_name, numeric_rank, mastery,
        level, stats["total"], stats["wins"], stats["winrate"]
    ))
    sf_conn.commit()
    cur.close()
    print(f"Inserted {summoner_name} for {date}")

def create_csv_row(summoner_name, tagline="NA1", region="americas", platform="na1"):
    puuid = get_puuid(summoner_name, tagline, region)
    date = datetime.now().strftime("%Y-%m-%d")
    rank = get_current_rank(puuid, platform)
    numrank = convert_rank_to_numeric(rank)
    mastery = get_champion_mastery_total(puuid, platform)
    level = get_summoner_level(puuid, platform)
    rankedstats = get_ranked_stats(puuid, platform)
    return f"{date},{summoner_name},{numrank},{mastery},{level},{rankedstats['total']},{rankedstats['wins']},{rankedstats['winrate']}"

collect_and_store("sacredswords15")