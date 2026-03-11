# to be run in Snowflake Streamlit App environment, not locally
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd
from snowflake.snowpark.functions import col

session = get_active_session()

st.title('LoL Visualization Tool')

st.write('A tool to visualize any player\'s League of Legends progress')

st.divider()

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

player = st.selectbox("Which player's stats?", get_unique())

options = ['LEVEL', 'MASTERY', 'RANK', 'RANKEDWINRATE', 'RANKEDWINS', 'TOTALRANKED']
selected_options = st.multiselect("What stats to see?", options)

data, query = generate_query([opt in selected_options for opt in options])

data = data[data['PLAYERID'] == player]

rankdata_long = data.melt('DATE', var_name='Measure', value_name='Value')

rankdata_long['Measure'] = rankdata_long['Measure'].replace({
    'LEVEL': 'Summoner Level',
    'MASTERY': 'Total Champion Mastery',
    'RANK': 'Rank as Numerical Value',
    'RANKEDWINRATE': 'Ranked Win Rate',
    'RANKEDWINS': 'Ranked Games Won',
    'TOTALRANKED': 'Ranked Games Played'
})

chart = get_chart(rankdata_long)

# Display the chart in the Streamlit app
st.altair_chart(chart, use_container_width=True)