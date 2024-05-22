import streamlit as st
import pymongo
import pandas as pd

st.set_page_config(
    page_title="Noticias",
    page_icon=":newspaper:",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongodb"])


client = init_connection()


# Pull data from the collection.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def get_data(date, database="noticias_db", collection="noticias"):
    db = client[database]

    filter_date = {"date": date}
    projection = {
        "_id": 0,
        "title": 1,
        "link": 1,
        "description": 1,
        "journal": 1,
        "sentiment": 1,
    }

    items = db[collection].find(filter_date, projection)
    items = list(items)  # make hashable for st.cache_data
    return items


# Define a function to create a horizontal bar
def sentiment_bar(sentiment):
    bar_html = f"""
    <div style="position: relative; height: 25px; width: 100%; background: linear-gradient(to right, red, yellow, green); border-radius: 12px;">
        <div style="position: absolute; left: {(sentiment + 1) * 50}%; top: 50%; transform: translate(-50%, -50%);">
            <svg height="20" width="20">
                <polygon points="10,0 0,20 20,20" style="fill:black;stroke:white;stroke-width:1" />
            </svg>
        </div>
    </div>
    """
    return bar_html


fecha = st.sidebar.date_input("Fecha", pd.to_datetime("2024-05-01"))

fecha_str = fecha.strftime("%Y_%m_%d")

items = get_data(fecha_str)

# Display the data.

st.title("Noticias")

if not items:
    st.write("### No hay noticias para esta fecha.")
    st.stop()

st.write(f"### Noticias para el {fecha_str.replace('_', '/')}")

for item in items:
    st.write(f"### {item['title']}")
    st.write(f"**Periódico:** {item['journal'].replace('_', ' ')}")
    st.write(f"**Descripción:** {item['description']}")
    st.write(f"**Sentiment:** {item['sentiment']}")
    # Dislay the sentiment with a horizontal bar
    # -1 is negative, 0 is neutral, 1 is positive
    # The bar should have a gradient from red to green
    # There should be an arrow pointing to the sentiment
    st.markdown(sentiment_bar(item["sentiment"]), unsafe_allow_html=True)
    st.write(f"[Link]({item['link']})")
    st.write("---")
