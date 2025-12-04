# generate_polygons.py
# Run this ONCE on your Render server after adding geopandas to requirements.txt
import os
import zipfile
import xml.etree.ElementTree as ET
from shapely.geometry import LineString
from shapely.ops import unary_union
import geopandas as gpd

# === AUTO-DETECT YOUR KMZ LOCATIONS (no manual paths!) ===
# Adjust these if your folder names are different
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # src folder
STATIC_FOLDER = os.path.join(PROJECT_ROOT, "static")

# Common places people put KMZs — we’ll search all of them
POSSIBLE_KMZ_DIRS = [
    os.path.join(PROJECT_ROOT, "kmz"),
    os.path.join(PROJECT_ROOT, "data"),
    os.path.join(PROJECT_ROOT, "kmzs"),
    os.path.join(PROJECT_ROOT, "FNA_members"),
    os.path.join(PROJECT_ROOT, "..", "kmz"),
    PROJECT_ROOT,
]

BLUEBIRD_KMZ = None
FNA_DIR = None

for folder in POSSIBLE_KMZ_DIRS:
    if os.path.exists(folder):
        # Look for Bluebird
        for f in os.listdir(folder):
            if f.lower().startswith("bluebird") and f.lower().endswith(".kmz"):
                BLUEBIRD_KMZ = os.path.join(folder, f)
            if os.path.isdir(os.path.join(folder, f)) and "fna" in f.lower():
                FNA_DIR = os.path.join(folder, f)

# Final fallback: ask user
if not BLUEBIRD_KMZ:
    BLUEBIRD_KMZ = input("Enter full path to Bluebird KMZ (e.g. /opt/render/project/src/kmz/Bluebird.kmz): ").strip()
if not FNA_DIR:
    FNA_DIR = input("Enter full path to FNA members folder: ").strip()

print(f"Using Bluebird: {BLUEBIRD_KMZ}")
print(f"Using FNA folder: {FNA_DIR}")

# Create output folder
os.makedirs(os.path.join(STATIC_FOLDER, "polygons"), exist_ok=True)

def extract_lines(kmz_path):
    lines = []
    try:
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
    except Exception as e:
        print(f"Error reading {kmz_path}: {e}")
    return lines

# === PROCESS BLUEBIRD ===
if BLUEBIRD_KMZ and os.path.exists(BLUEBIRD_KMZ):
    print("Processing Bluebird Network...")
    lines = extract_lines(BLUEBIRD_KMZ)
    if lines:
        union = unary_union(lines)
        buffered = union.buffer(0.18)          # ~12 miles — looks perfect
        simplified = buffered.simplify(0.015)  # smooth but accurate
        gdf = gpd.GeoDataFrame(geometry=[simplified], crs="EPSG:4326")
        gdf.to_file(os.path.join(STATIC_FOLDER, "polygons/Bluebird_Network.geojson"), driver="GeoJSON")
        print("✓ Bluebird_Network.geojson created")
    else:
        print("No lines found in Bluebird KMZ")

# === PROCESS ALL FNA MEMBERS ===
if FNA_DIR and os.path.exists(FNA_DIR):
    print(f"Processing {len([f for f in os.listdir(FNA_DIR) if f.lower().endswith('.kmz')])} FNA members...")
    for f in sorted(os.listdir(FNA_DIR)):
        if not f.lower().endswith('.kmz'):
            continue
        name = os.path.splitext(f)[0].replace('_', ' ').title()
        path = os.path.join(FNA_DIR, f)
        print(f"  → {name}")
        lines = extract_lines(path)
        if lines:
            union = unary_union(lines)
            buffered = union.buffer(0.18)
ordinary            simplified = buffered.simplify(0.015)
            gdf = gpd.GeoDataFrame(geometry=[simplified], crs="EPSG:4326")
            safe_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in name)
            gdf.to_file(os.path.join(STATIC_FOLDER, "polygons", f"{safe_name}.geojson"), driver="GeoJSON")
        else:
            print(f"    No lines in {name}")

print("\n=== ALL DONE! ===")
print("Your beautiful coverage polygons are in:")
print(os.path.join(STATIC_FOLDER, "polygons"))
print("\nRestart your Flask app (or just wait for Render redeploy)")
print("Then open the map — you’re about to see something stunning.")
