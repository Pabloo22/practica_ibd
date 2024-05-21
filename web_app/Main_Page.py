import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from scipy.spatial import Voronoi
import branca.colormap as cm
import numpy as np
import plotly.express as px
from utils.voronoi import create_regions
from collections import defaultdict

scales_dict = {
    1: [0, 40],
    2: [0, 10],
    3: [0, 30],
    4: [0, 40],
    5: [0, 25],
    6: [0, 15],
    7: [0, 30],
    8: [0, 100],
    9: [0, 5],
    10: [0, 5],
    11: [0, 5],
    12: [0, 5],
    13: [0, 5],
    14: [0, 5],
    15: [0, 2],
    16: [0, 2],
    17: [0, 1],
    18: [0, 20],
    19: [0, 360],
    20: [0, 40],
    21: [0, 100],
    22: [890, 990],
    23: [0, 1000],
    24: [0, 100],
    25: [20, 110],
    26: [20, 110],
    27: [20, 110],
    28: [20, 110],
    29: [20, 110],
    30: [20, 110],
    34: [0, 40],
    35: [0, 100],
}

SCALES = defaultdict(lambda: [0, 100], scales_dict)


def get_measurement(station_id, calidad_aire):
    measurement = calidad_aire[(calidad_aire["station_id"] == station_id)][
        "measure"
    ].values[0]
    return measurement


st.set_page_config(
    page_title="Main Page",
    page_icon=":world_map:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Main Page")

conn = st.connection("postgresql", type="sql")
query_metricas = """
SELECT *
FROM dim_metric
"""

metricas = conn.query(query_metricas)
dict_metrics = metricas.set_index("metric_id").to_dict()["metric_name"]

metric = st.sidebar.selectbox(
    "Select metric",
    dict_metrics.keys(),
    format_func=lambda x: dict_metrics[x],
)


fecha = st.sidebar.date_input("Fecha", pd.to_datetime("2024-03-01"))
anio = fecha.year
mes = fecha.month
dia = fecha.day

datetime = pd.to_datetime(f"{anio}-{mes}-{dia} 00:00:00")

query_medidas = f"""
SELECT *
FROM fact_measure
WHERE date = '{datetime}' AND metric_id = {metric}
"""

medidas = conn.query(query_medidas)

estaciones_en_medidas = medidas["station_id"].unique()

query_estaciones = """
SELECT *
FROM dim_station
"""

estaciones = conn.query(query_estaciones)

estaciones = estaciones[estaciones["station_id"].isin(estaciones_en_medidas)]

coords = estaciones[["latitude", "longitude"]].to_numpy()
# multiply the second column by -1 to invert the coordinates
coords[:, 1] = coords[:, 1] * -1
ids = estaciones["station_id"].values
station_names = estaciones["station_name"].values

try:
    regions = create_regions(coords, ids, station_names)
except Exception as e:
    "# There is no data"
    st.stop()

m = folium.Map()

vmin = SCALES[metric][0]
vmax = SCALES[metric][1]

cmap = cm.LinearColormap(["green", "yellow", "red"], vmin=vmin, vmax=vmax)

for i, r in enumerate(regions):
    if r.is_inf:
        continue
    try:
        measurement = get_measurement(r.station_id, medidas)
        color = cmap(measurement)
    except Exception as e:
        print(e)
        color = "gray"
        measurement = -1
    folium.Polygon(
        locations=r.vertices,
        color=color,
        fill_color=color,
        fill_opacity=0.5,
        popup=r.name,
        tooltip=measurement,
    ).add_to(m)
    folium.Marker(
        r.coords, icon=folium.Icon(color="blue"), popup=r.name
    ).add_to(m)
m.fit_bounds(m.get_bounds())

st_data = st_folium(
    m,
    # returned_objects=["last_object_clicked_tooltip"],
    height=1000,
    width=1000,
)

if st_data["last_object_clicked_popup"]:
    # First, look for the station_id in the popup
    station_name = st_data["last_object_clicked_popup"]
    station_id = estaciones[estaciones["station_name"] == station_name][
        "station_id"
    ].values[0]
    # Then, filter the DataFrame by the station_id
    query_historico_estacion = f"""
    SELECT *
    FROM fact_measure
      station_id = {station_id} AND metric_id = {metric}
    """
    historico_estacion = conn.query(query_historico_estacion)
    # Plot in a line chart
    metric_name = dict_metrics[metric]
    f"# Historical data: for the station {station_name}"
    fig = px.line(
        historico_estacion,
        x="date",
        y="measure",
        title=f"Measurements for the station {station_name}",
        labels={"measure": metric_name, "date": "Date"},
    )
    st.plotly_chart(fig)
