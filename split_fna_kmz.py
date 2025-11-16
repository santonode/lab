# split_fna_kmz.py — Split AllMemberFiber.kmz by FNA Member
import zipfile
import xml.etree.ElementTree as ET
import os
import sys

KMZ_IN = "AllMemberFiber.kmz"
OUTPUT_DIR = "fna_members"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"Reading {KMZ_IN}...")

with zipfile.ZipFile(KMZ_IN, 'r') as kmz:
    kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
    if not kml_files:
        print("No .kml found in KMZ")
        sys.exit(1)
    kml_data = kmz.read(kml_files[0])

print("Parsing KML...")
root = ET.fromstring(kml_data)
ns = {'kml': 'http://www.opengis.net/kml/2.2'}

# Group placemarks by member (from <name>)
members = {}
for placemark in root.findall('.//kml:Placemark', ns):
    name_elem = placemark.find('kml:name', ns)
    if name_elem is None or not name_elem.text:
        continue
    member_name = name_elem.text.strip()
    if not member_name or member_name == "Unnamed":
        continue
    if member_name not in members:
        members[member_name] = []
    members[member_name].append(placemark)

print(f"Found {len(members)} FNA members")

# Create one KMZ per member
for member, placemarks in members.items():
    safe_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in member)[:50]
    kmz_out = os.path.join(OUTPUT_DIR, f"{safe_name}.kmz")
    
    with zipfile.ZipFile(kmz_out, 'w', zipfile.ZIP_DEFLATED) as out_kmz:
        # Create new KML
        doc = ET.Element('kml', xmlns="http://www.opengis.net/kml/2.2")
        document = ET.SubElement(doc, 'Document')
        name = ET.SubElement(document, 'name')
        name.text = member
        
        for pm in placemarks:
            document.append(pm)
        
        kml_bytes = ET.tostring(doc, encoding='utf-8')
        out_kmz.writestr('doc.kml', kml_bytes)
    
    print(f"→ {kmz_out} ({len(placemarks)} features)")

print(f"\nDone! {len(members)} member KMZ files in '{OUTPUT_DIR}/'")
print("Next: Update erate.py to load from this folder")
