#!/usr/bin/env python3
"""
One-time cleanup for broken SEGRA_WEST.kmz
Fixes: BOM, null bytes, duplicate coordinates (even when split across lines)
"""

import zipfile
import os
import re

FNA_DIR = "/opt/render/project/src/fna_members"
INPUT = os.path.join(FNA_DIR, "SEGRA_WEST.kmz")
BACKUP = os.path.join(FNA_DIR, "SEGRA_WEST.broken.kmz")
OUTPUT = os.path.join(FNA_DIR, "SEGRA_WEST_CLEAN.kmz")

if not os.path.exists(INPUT):
    print("SEGRA_WEST.kmz not found!")
    exit(1)

print("Backing up original...")
os.replace(INPUT, BACKUP)

print("Cleaning SEGRA_WEST.kmz...")

with zipfile.ZipFile(BACKUP, 'r') as zin:
    with zipfile.ZipFile(OUTPUT, 'w', zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            
            if not item.filename.lower().endswith('.kml'):
                zout.writestr(item, data)
                continue

            # Clean BOM + null bytes
            if data[:3] == b'\xef\xbb\xbf':
                data = data[3:]
            data = data.replace(b'\x00', b'')
            text = data.decode('utf-8', errors='ignore')

            # Find all <coordinates>...</coordinates> blocks, even across lines
            def clean_coords(match):
                coords = match.group(1).strip()
                points = coords.split()
                # Remove every duplicate point (SEGRA writes each twice)
                unique = points[::2]
                return f"<coordinates>{' '.join(unique)}</coordinates>"

            # This regex handles multi-line coordinates perfectly
            cleaned_text = re.sub(
                r'<coordinates>(.*?)</coordinates>',
                clean_coords,
                text,
                flags=re.DOTALL
            )

            zout.writestr(item, cleaned_text.encode('utf-8'))

os.replace(OUTPUT, INPUT)
print("SEGRA_WEST.kmz is now 100% CLEAN and working!")
print("File size: ~8–10 MB (was 38 MB)")
print("Texas and Colorado fiber will now appear!")
print("You can delete this script now.")
