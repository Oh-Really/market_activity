import streamlit as st
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
st.title("Trade Activity")


DB_HOST = "postgres2"  # Docker service name as hostname
DB_PORT = "5432"       # Internal container port
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None
    
# Query example
connection = get_connection()
try:
    query = """SELECT "instrumentId",
    instruments.name AS instName,
    CASE WHEN CAST(LEFT(CAST("sequenceId" AS VARCHAR(100)), 6) AS INT) = 100003 THEN 'Gas'
    WHEN CAST(LEFT(CAST("sequenceId" AS VARCHAR(100)), 6) AS INT) = 100001 THEN 'Power'
    WHEN CAST(LEFT(CAST("sequenceId" AS VARCHAR(100)), 6) AS INT) = 100004 THEN 'Emissions'
    WHEN CAST(LEFT(CAST("sequenceId" AS VARCHAR(100)), 7) AS INT) = 1000002 THEN 'Power'
    WHEN CAST(LEFT(CAST("sequenceId" AS VARCHAR(100)), 7) AS INT) = 1000000 THEN 'Power'
    WHEN CAST(LEFT(CAST("sequenceId" AS VARCHAR(100)), 6) AS INT) = 100005 THEN 'Coal'
    END AS Commodity,
    "sequenceItemId",
    "contractType",
    trades.count
    from trades
    inner join instruments ON trades."instrumentId" = instruments."id";
            """
    df = pd.read_sql(query, connection)
    st.write("Trades:")
    st.dataframe(df)
except Exception as e:
    st.error(f"Failed to run query: {e}")

st.bar_chart(df, x='commodity', y='count')

labels = pd.unique(df['commodity'])
labels = labels[labels != None]
sizes = df.groupby('commodity')['count'].sum()

st.write(sizes)
st.write(labels)

fig1, ax1 = plt.subplots()
ax1.pie(sizes.to_list(), labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

st.pyplot(fig1)