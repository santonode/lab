# Map4.net E-Rate Query Tool

Fast, mobile-first web application for searching and analyzing live USAC E-Rate 470/471 open data.

Live at: https://lab.santoelectronics.com/erate

## Features
- Real-time search of all active FCC Form 470s from USAC open data
- Geographic mapping with Bluebird Network and FNA fiber overlay (57+ KMZ files parsed live)
- Distance-to-fiber calculation (Bluebird or FNA member-specific)
- Guest mode + pay-as-you-go points system (deducts 1 point per map/distance click)
- Registered users get higher limits and persistent points
- Authentication via SHA256-hashed member passwords (no plaintext ever stored)
- Full export to CSV when result set ≤ user’s limit
- Responsive design – works great on phones and tablets

## Database Schema (PostgreSQL)
<pre>
| Name             | Type  | Owner
|------------------|-------|----------------
| erate            | table | wurdle_db_user
| erate_hash       | table | wurdle_db_user  
| import_hash_log  | table | wurdle_db_user
| users            | table | wurdle_db_user
| user_stats       | table | wurdle_db_user
| daily_word       | table | wurdle_db_user ← (legacy, will be dropped)
| game_logs        | table | wurdle_db_user ← (legacy, will be dropped)
| memes            | table | wurdle_db_user ← (legacy, will be dropped)
| votes            | table | wurdle_db_user ← (legacy, will be dropped)
</pre>

## Project Structure
<pre>
/src
├── app.py                     # Main Flask app
├── db.py                      # Database connection & queries
├── erate.py                   # All E-Rate routes and logic
├── models.py                  # SQLAlchemy models
├── split_fna_kmz.py           # Script that parses and splits FNA KMZ per member
├── recreate_erate.sql         # Full schema + indexes for fresh deploy
├── render.yaml                # Render.com deployment config
├── requirements.txt
└── /templates
    ├── erate.html            # Main dashboard
    ├── eadmin.html           # Admin/import page
    ├── erate_import.html
    └── erate_import_complete.html
  
</pre>
