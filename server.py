import random  
import dash  
from dash import html  
import dash_leaflet as dl  
import dash_leaflet.express as dlx  
from dash_extensions.javascript import assign
import sqlite3
from record_link import get_lat_long, get_header
import pandas as pd

conn = sqlite3.connect("proj_ez.sqlite3")
cur = conn.cursor()
cur.execute("SELECT * FROM 'mock'")
df = cur.fetchall()
df = pd.DataFrame(df, columns=get_header(cur))
get_lat_long(df)
points = []
for i,row in df.iterrows():
    if row["lat_long"] != (None,None):
        points.append(dict(lat=row["lat_long"][0],lon=row["lat_long"][1],value=row["primary_type"]))
    
# Create some markers.  
data = dlx.dicts_to_geojson(points)  
# Create geojson.  
point_to_layer = assign("function(feature, latlng, context) {return L.marker(latlng);}")
bind = assign("function(feature,layer) {layer.bindPopup(feature.properties.value);}")
geojson = dl.GeoJSON(data=data, options=dict(pointToLayer=point_to_layer, onEachFeature=bind))

# Create the app.  
app = dash.Dash()  
app.layout = html.Div([  
    dl.Map([dl.TileLayer(), geojson], center=(41.8691,-87.6298), zoom=12, style={'height': '100vh'}),  
])  

if __name__ == '__main__':  
    app.run_server()