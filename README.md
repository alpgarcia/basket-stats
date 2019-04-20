# basket-stats
Basketball stats playground based on Python, ElasticSearch
and Kibana.

Available stuff up to now includes:

* [Scripts](#scripts): available Python scripts.
* [Dashboards](#dashboards)
  * [Kibana 7](#kibana-7)

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

## Dashboards

### Kibana 7

#### Player Stats

A [sample dashboard on players stats](dashboards/kibana7/PlayerStats.json)
is included in the repo, ready to be imported into Kibana from
`Management (left hand side vertical menu, last item at the bottom)
 -> Saved Objects -> Import (top right corner, just next to 
 'Refresh')`. 
 
Once imported, it will look like this:

![Player Stats Sample Dashboard](assets/screenshots/player_stats_sample_dashboard.png)