import random  
import dash  
import dash_html_components as html  
import dash_leaflet as dl  
import dash_leaflet.express as dlx  
from dash_extensions.javascript import assign

# Create some markers.  
points = [dict(lat=41.87 + random.random(), lon=-87.63 + random.random(), value=random.random()) for i in range(100)]  
data = dlx.dicts_to_geojson(points)  
# Create geojson.  
point_to_layer = assign("function(feature, latlng, context) {return L.circleMarker(latlng);}")
geojson = dl.GeoJSON(data=data, options=dict(pointToLayer=point_to_layer))
# Create the app.  
app = dash.Dash()  
app.layout = html.Div([  
    dl.Map([dl.TileLayer(), geojson], center=(41,-87), zoom=8, style={'height': '100vh'}),  
])  

if __name__ == '__main__':  
    app.run_server()