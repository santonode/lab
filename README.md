# Santo E-Rate Query Tool

Fast, mobile-first web application for searching and analyzing live USAC E-Rate 470/471 open data.

Live at: https://erate.santoelectronics.com

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

| Name             | Type  | Owner
|------------------|-------|----------------
| public           | erate            | table | wurdle_db_user
| public           | erate_hash       | table | wurdle_db_user  
| public           | import_hash_log  | table | wurdle_db_user
| public           | users            | table | wurdle_db_user
| public           | user_stats       | table | wurdle_db_user
| public           | daily_word       | table | wurdle_db_user ← (legacy, will be dropped)
| public           | game_logs        | table | wurdle_db_user ← (legacy)
| public           | memes            | table | wurdle_db_user ← (legacy)
| public           | votes            | table | wurdle_db_user ← (legacy)

## Project Structure
