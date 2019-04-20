# basket-stats
Basketball stats playground

## Scripts

### upload_to_es.py

Reads seasons stats from CSV file and imports them into 
ElasticSearch. All the data needed is properly stored in
the repo.

Code tested against:
* ElasticSearch 7.0.0
* Kibana 7.0.0

As output the script creates an index named `season_stats`
with information coming from 
[Seasons_Stats.csv](data/nba-players-stats/Seasons_Stats.csv),
ready to be queried and used in Kibana (see 
[mapping](mappings/season_stats.json)).

Some example visualizations built on Kibana 7:

![Top Scorers Heatmap](assets/screenshots/top_scorers_heatmap.png)

![Top Scorers Table](assets/screenshots/top_scorers_table.png)


