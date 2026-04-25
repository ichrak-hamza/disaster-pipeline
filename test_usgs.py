import requests

url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
r = requests.get(url)
data = r.json()

print(f"Statut : {r.status_code}")
print(f"Nombre de séismes cette heure : {data['metadata']['count']}")
print(f"Premier séisme : {data['features'][0]['properties']['title']}")
