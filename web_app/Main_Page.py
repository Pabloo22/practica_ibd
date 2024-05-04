import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from scipy.spatial import Voronoi
import branca.colormap as cm
import numpy as np
import plotly.express as px
from utils.voronoi import create_regions


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

metric = st.selectbox(
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

calidad_aire = conn.query(query_medidas)

query_estaciones = """
SELECT *
FROM dim_station
"""

estaciones = conn.query(query_estaciones)

coords = estaciones[["latitude", "longitude"]].to_numpy()
# multiply the second column by -1 to invert the coordinates
coords[:, 1] = coords[:, 1] * -1
ids = estaciones["station_id"].values
station_names = estaciones["station_name"].values


regions = create_regions(coords, ids, station_names)

m = folium.Map()

min_measurement = calidad_aire["measure"].min()
max_measurement = calidad_aire["measure"].max()

cmap = cm.LinearColormap(
    ["green", "yellow", "red"], vmin=min_measurement, vmax=max_measurement
)

for i, r in enumerate(regions):
    if r.is_inf:
        continue
    try:
        measurement = get_measurement(r.station_id, calidad_aire)
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
        popup=measurement,
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
    WHERE station_id = {station_id} AND metric_id = {metric}
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
