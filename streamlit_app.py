import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd
from snowflake.snowpark.functions import col

session = get_active_session()

st.title('LoL Visualization Tool')
st.write('A tool to visualize any player\'s League of Legends progress')

# ── Add Player ───────────────────────────────────────────────────────────────
st.divider()
st.subheader("Add a New Player")

REGION_OPTIONS = {
    "NA  (americas / na1)":  ("americas", "na1"),
    "EUW (europe / euw1)":   ("europe",   "euw1"),
    "EUNE (europe / eun1)":  ("europe",   "eun1"),
    "KR  (asia / kr)":       ("asia",     "kr"),
    "JP  (asia / jp1)":      ("asia",     "jp1"),
    "BR  (americas / br1)":  ("americas", "br1"),
    "LAN (americas / la1)":  ("americas", "la1"),
    "LAS (americas / la2)":  ("americas", "la2"),
    "OCE (sea / oc1)":       ("sea",      "oc1"),
}

with st.form("add_player_form"):
    col1, col2 = st.columns(2)
    with col1:
        new_playerid = st.text_input("Summoner Name", placeholder="e.g. TFBlade")
    with col2:
        new_tagline  = st.text_input("Tagline", placeholder="e.g. NA1")

    region_label = st.selectbox("Region", list(REGION_OPTIONS.keys()))
    submitted    = st.form_submit_button("Add Player")

if submitted:
    if not new_playerid or not new_tagline:
        st.warning("Please fill in both Summoner Name and Tagline.")
    else:
        region, platform = REGION_OPTIONS[region_label]
        try:
            result = session.call(
                "RANKEDDATA.LOLSCHEMA.ADD_PLAYER",
                new_playerid.strip(),
                new_tagline.strip(),
                region,
                platform
            )
            if result.startswith("OK"):
                st.success(result)
                st.cache_data.clear()  # refresh player dropdown
            elif result.startswith("SKIPPED"):
                st.warning(result)
            else:
                st.error(result)
        except Exception as e:
            st.error(f"Failed to call stored procedure: {e}")

st.divider()
st.subheader("Visualization Tool")

@st.cache_data
def get_players_data(player_name, query):
    sql = f"""
        SELECT""" + query + """
    FROM lol_player_stats WHERE PLAYERID in ({player_name});
    """
    ranked_data = session.sql(sql).to_pandas()
    return ranked_data, sql

def get_chart(rankdata_long):
    chart = alt.Chart(rankdata_long).mark_line(point=True).encode(
        x=alt.X('DATE:T', title='Date'),
        y=alt.Y('Value:Q', title='Values'),
        color=alt.Color('Measure:N', title='Legend', scale=alt.Scale(
            range=['#29B5E8', '#FF6F61', '#0072CE', '#FFC300', '#E84B3A', '#4B7BE8']
        )),
        tooltip=['DATE:T', 'Measure:N', 'Value:Q']
    ).interactive().properties(
        width=700,
        height=400,
        title='LoL Visualizations'
    ).configure_title(
        fontSize=20,
        font='Arial'
    ).configure_axis(
        grid=True
    ).configure_view(
        strokeWidth=0
    )
    return chart

def generate_query(params):
    columns = ["DATE", "PLAYERID"]
    if params[0]: columns.append("LEVEL")
    if params[1]: columns.append("MASTERY")
    if params[2]: columns.append("RANK")
    if params[3]: columns.append("RANKEDWINRATE")
    if params[4]: columns.append("RANKEDWINS")
    if params[5]: columns.append("TOTALRANKED")
    query = ", ".join(columns)
    
    data = session.table("RANKEDDATA.LOLSCHEMA.VisualsTable").select(
        *[col(c) for c in columns]
    ).to_pandas()
    return data, query

@st.cache_data
def get_unique():
    sql = "SELECT DISTINCT PlayerID FROM RANKEDDATA.LOLSCHEMA.VisualsTable"
    players = session.sql(sql).to_pandas()
    return players

# ── Visualization ───────────────────────────────────────────────────────────
player = st.selectbox("Which player's stats?", get_unique())
options = ['LEVEL', 'MASTERY', 'RANK', 'RANKEDWINRATE', 'RANKEDWINS', 'TOTALRANKED']
selected_options = st.multiselect("What stats to see?", options)

data, query = generate_query([opt in selected_options for opt in options])
data = data[data['PLAYERID'] == player]
stat_columns = [c for c in data.columns if c not in ('DATE', 'PLAYERID')]
rankdata_long = data.melt('DATE', value_vars=stat_columns, var_name='Measure', value_name='Value')
rankdata_long['Measure'] = rankdata_long['Measure'].replace({
    'LEVEL': 'Summoner Level',
    'MASTERY': 'Total Champion Mastery',
    'RANK': 'Rank as Numerical Value',
    'RANKEDWINRATE': 'Ranked Win Rate',
    'RANKEDWINS': 'Ranked Games Won',
    'TOTALRANKED': 'Ranked Games Played'
})

chart = get_chart(rankdata_long)
st.altair_chart(chart, use_container_width=True)

