import os
from dotenv import load_dotenv
import requests

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")

def get_puuid(summoner_name, tagline="NA1", region="americas", api_key="YOUR_KEY"):
    """
    Get PUUID for a player via Riot Account API
    
    Args:
        summoner_name: Riot ID name (without #tag)
        tagline: Riot ID tagline e.g. "NA1"
        region: Regional cluster (americas, europe, asia, sea)
        api_key: Riot Games API key
    
    Returns:
        puuid string
    """
    headers = {"X-Riot-Token": api_key}
    url = f"https://{region}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{summoner_name}/{tagline}"
    
    r = requests.get(url, headers=headers)
    r.raise_for_status()

def get_current_rank(puuid, platform="na1", api_key="YOUR_KEY"):
    headers = {"X-Riot-Token": api_key}
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    
    for entry in r.json():
        if entry["queueType"] == "RANKED_SOLO_5x5":
            return f"{entry['tier']} {entry['rank']} - {entry['leaguePoints']} LP"
    
    return "Unranked"

def convert_rank_to_numeric(rank_str):
    rank, tier, dummy1, lp, dummy2 = rank_str.split()

    print(f"Parsed rank: {rank}, tier: {tier}, LP: {lp}")

    tier_value = {
        "IRON": 0,
        "BRONZE": 400,
        "SILVER": 800,
        "GOLD": 1200,
        "PLATINUM": 1600,
        "DIAMOND": 2000,
        "MASTER": 2400,
        "GRANDMASTER": 2400,
        "CHALLENGER": 2400
    }.get(rank, -1)

    division_value = {
        "IV": 0,
        "III": 100,
        "II": 200,
        "I": 300
    }.get(tier, -1)
    
    lp_value = int(lp)

    if tier_value == -1 or division_value == -1:
        raise ValueError(f"Invalid rank string: {rank_str}")
    return tier_value + division_value + lp_value