'''
This file is made to connect and pull mock ECCSC data,
convert it into markers in dash-leaflet, and open a server
to plot the clustered points on a map with labels
'''
import random
import sqlite3
import pandas as pd
import dash
from dash import html
import dash_leaflet as dl
import dash_leaflet.express as dlx
from dash_extensions.javascript import assign
from record_link import get_lat_long, get_header, go
from ez.refresh_data import sql_query


mock_df = sql_query.create_survery_df()

points = []
for i,row in mock_df.iterrows():
    if row["lat_long"] != (None,None):
        points.append(dict(lat=row["lat_long"][0],lon=row["lat_long"][1],value=row["primary_type"]))

# Create markers
data = dlx.dicts_to_geojson(points)
# Create geojson.
point_to_layer = assign("function(feature, latlng, context) {return L.marker(latlng);}")
bind = assign("function(feature,layer) {layer.bindPopup(feature.properties.value);}")
geojson = dl.GeoJSON(data=data, cluster=True, zoomToBoundsOnClick=True,
                     options=dict(pointToLayer=point_to_layer, onEachFeature=bind))

# Create the app.
app = dash.Dash()
app.layout = html.Div([
    dl.Map(id = "map1",children = [dl.TileLayer(id="title1"),geojson], center=(41.8691,-87.6298), zoom=12, style={'height': '100vh'}),
])
if __name__ == '__main__':
    app.run_server()
