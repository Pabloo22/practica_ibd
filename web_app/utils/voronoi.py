import folium
import pandas as pd
from scipy.spatial import Voronoi
import branca.colormap as cm
import numpy as np


class VoronoiRegion:
    def __init__(self, region_id, station_id, name):
        self.id = region_id
        self.station_id = station_id
        self._name = name
        self.vertices = []
        self.is_inf = False
        self.point_inside = None

    def __str__(self):
        text = f"region id={self.id}" + f"({self.station_id})"
        if self.point_inside:
            point_idx, point = self.point_inside
            text = f"{text}[point:{point}(point_id:{point_idx})]"
        text += ", vertices: "
        if self.is_inf:
            text += "(inf)"
        for v in self.vertices:
            text += f"{v}"
        return text

    def __repr__(self):
        return str(self)

    def add_vertex(self, vertex, vertices):
        if vertex == -1:
            self.is_inf = True
        else:
            point = vertices[vertex]
            self.vertices.append(point)

    @property
    def name(self):
        return self._name

    @property
    def coords(self):
        return self.point_inside[1]


def voronoi_to_voronoi_regions(voronoi, ids, station_names):
    voronoi_regions = []
    if len(ids) != len(voronoi.points):
        # Append None to the names list
        ids = np.append(ids, [None] * (len(voronoi.points) - len(ids)))

    for i, (point_region, station_id, name) in enumerate(
        zip(voronoi.point_region, ids, station_names)
    ):
        region = voronoi.regions[point_region]
        vr = VoronoiRegion(point_region, station_id, name)
        for r in region:
            vr.add_vertex(r, voronoi.vertices)
        vr.point_inside = (i, voronoi.points[i])
        voronoi_regions.append(vr)
    return voronoi_regions


def create_regions(coords, ids, station_names):
    min_lat = coords[:, 0].min() - 0.2
    max_lat = coords[:, 0].max() + 0.2
    min_lon = coords[:, 1].min() - 0.2
    max_lon = coords[:, 1].max() + 0.2

    vor = Voronoi(
        np.append(
            coords,
            [
                [min_lat, min_lon],
                [min_lat, max_lon],
                [max_lat, min_lon],
                [max_lat, max_lon],
            ],
            axis=0,
        )
    )
    regions = voronoi_to_voronoi_regions(vor, ids, station_names)
    return regions
