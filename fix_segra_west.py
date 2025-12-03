#!/usr/bin/env python3
"""
One-time cleanup script for broken SEGRA_WEST.kmz
Run once on the server to fix duplicate coordinates, BOM, null bytes.
After running, delete this file — it's no longer needed.
"""

import zipfile
import os

FNA_DIR = "/opt/render/project/src/fna_members"
INPUT = os.path.join(FNA_DIR, "SEGRA_WEST.kmz")
OUTPUT = os.path.join(FNA_DIR, "SEGRA_WEST_CLEAN.kmz")
BACKUP = os.path.join(FNA_DIR, "SEGRA_WEST.broken.kmz")

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

            # Fix duplicate coordinates (SEGRA bug: every point written twice)
            lines = text.splitlines()
            cleaned = []
            in_coords = False
            for line in lines:
                if '<coordinates>' in line.lower():
                    in_coords = True
                    before, rest = line.split('<coordinates>', 1)
                    coords, after = rest.split('</coordinates>', 1)
                    points = coords.strip().split()
                    # Remove every second point (the duplicate)
                    unique = points[::2]
                    cleaned_line = f"{before}<coordinates>{' '.join(unique)}</coordinates>{after}"
                    cleaned.append(cleaned_line)
                elif '</coordinates>' in line.lower():
                    in_coords = False
                    cleaned.append(line)
                else:
                    cleaned.append(line)

            zout.writestr(item, '\n'.join(cleaned).encode('utf-8'))

os.replace(OUTPUT, INPUT)
print("SEGRA_WEST.kmz is now CLEAN and working!")
print("Old broken version saved as SEGRA_WEST.broken.kmz")
print("You can now delete this script.")
