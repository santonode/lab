# generate_polygons.py
import os
import zipfile
import xml.etree.ElementTree as ET
import geopandas as gpd
from shapely.geometry import LineString
from shapely.ops import unary_union
import json

# <<< PUT YOUR SAME PATHS HERE >>>
KMZ_PATH_BLUEBIRD = "path/to/your/Bluebird.kmz"
FNA_MEMBERS_DIR = "path/to/your/FNA_members_folder"
STATIC_FOLDER = "src/static"  # Flask static folder

os.makedirs(os.path.join(STATIC_FOLDER, "polygons"), exist_ok=True)

def extract_lines(kmz_path):
    lines = []
    with zipfile.ZipFile(kmz_path, 'r') as z:
        kml_files = [f for f in z.namelist() if f.lower().endswith('.kml')]
        if not kml_files:
            return lines
        root = ET.fromstring(z.read(kml_files[0]))
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        for coord_elem in root.findall('.//kml:LineString/kml:coordinates', ns):
            if not coord_elem.text:
                continue
            coords = []
            for token in coord_elem.text.strip().split():
                parts = token.split(',')
                if len(parts) >= 2:
                    try:
                        lon, lat = float(parts[0]), float(parts[1])
                        coords.append((lon, lat))
                    except:
                        continue
            if len(coords) >= 2:
                lines.append(LineString(coords))
    return lines

# Bluebird
lines = extract_lines(KMZ_PATH_BLUEBIRD)
if lines:
    union = unary_union(lines)
    buffered = union.buffer(0.15)  # ~10 miles
    simplified = buffered.simplify(0.02)
    gdf = gpd.GeoDataFrame(geometry=[simplified], crs="EPSG:4326")
    gdf.to_file(os.path.join(STATIC_FOLDER, "polygons/Bluebird_Network.geojson"), driver="GeoJSON")
    print("Bluebird polygon saved")

# FNA Members
colors = ["#dc3545","#28a745","#fd7e14","#6f42c1","#20c997","#e83e8c","#6610f2","#17a2b8","#ffc107","#6c757d"]
idx = 0
for f in sorted(os.listdir(FNA_MEMBERS_DIR)):
    if f.lower().endswith('.kmz'):
        name = os.path.splitext(f)[0].replace('_', ' ').title()
        path = os.path.join(FNA_MEMBERS_DIR, f)
        lines = extract_lines(path)
        if lines:
            union = unary_union(lines)
            buffered = union.buffer(0.15)
            simplified = buffered.simplify(0.02)
            gdf = gpd.GeoDataFrame(geometry=[simplified], crs="EPSG:4326")
            filename = f"{name.replace(' ', '_')}.geojson"
            gdf.to_file(os.path.join(STATIC_FOLDER, "polygons", filename), driver="GeoJSON")
            print(f"{name} polygon saved")
        idx += 1

print("\nAll done! Polygons are in src/static/polygons/")
print("Restart Flask — your map now has beautiful coverage areas!")
