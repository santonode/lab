# Santo E-Rate Query Tool
### Web Application for querying the USAC.org open data system. Designed for Mobile first Desktop second. Hosted on Render.com to manage python packages and postgess db instance. Uses a pay as you go user authentication sysem to allow anon guest users with SHA256 member passwords. 
<pre>
                   List of tables
 Schema |      Name       | Type  |     Owner      
--------+-----------------+-------+----------------
 public | daily_word (x)  | table | wurdle_db_user
 public | erate           | table | wurdle_db_user
 public | erate_hash      | table | wurdle_db_user
 public | game_logs (x)   | table | wurdle_db_user
 public | import_hash_log | table | wurdle_db_user
 public | memes (x)       | table | wurdle_db_user
 public | user_stats      | table | wurdle_db_user
 public | users           | table | wurdle_db_user
 public | votes (x)       | table | wurdle_db_user
  x=flagged for removal
  
App File Structure
/src
  app.py
  db.py
  erate.py
  models.py
  split_fna_kmz.py
  recreate_erate.sql
  render.yaml
  requirements.txt
  /templates
    erate.html
    eadmin.html
    erate_import.html
    erate_import_complete.html
</pre>
