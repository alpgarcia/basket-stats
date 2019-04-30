# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Alberto Pérez García-Plaza
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Alberto Pérez García-Plaza <alpgarcia@gmail.com>
#


import csv
import hashlib
import os
import sys

from elasticsearch import Elasticsearch, helpers

SEASON_STATS = 'season_stats'

# Columns short to long names
column_names = {
    'Year': 'Season',
    'Player': 'name',
    'Pos': 'Position',
    'Age': 'Age',
    'Tm': 'Team',
    'G': 'Games',
    'GS': 'Games Started',
    'MP': 'Minutes Played',
    'PER': 'Player Efficiency Rating',
    'TS%': 'True Shooting Percentage',
    '3PAr': '3-Point Attempt Rate',
    'FTr': 'Free Throw Rate',
    'ORB%': 'Offensive Rebound Percentage',
    'DRB%': 'Defensive Rebound Percentage',
    'TRB%': 'Total Rebound Percentage',
    'AST%': 'Assist Percentage',
    'STL%': 'Steal Percentage',
    'BLK%': 'Block Percentage',
    'TOV%': 'Turnover Percentage',
    'USG%': 'Usage Percentage',
    # 'blanl': 'empty',
    'OWS': 'Offensive Win Shares',
    'DWS': 'Defensive Win Shares',
    'WS': 'Win Shares',
    'WS/48': 'Win Shares Per 48 Minutes',
    # 'blank2': 'empty2',
    'OBPM': 'Offensive Box Plus/Minus',
    'DBPM': 'Defensive Box Plus/Minus',
    'BPM': 'Box Plus/Minus',
    'VORP': 'Value Over Replacement',
    'FG': 'Field Goals',
    'FGA': 'Field Goal Attempts',
    'FG%': 'Field Goal Percentage',
    '3P': '3-Point Field Goals',
    '3PA': '3-Point Field Goal Attempts',
    '3P%': '3-Point Field Goal Percentage',
    '2P': '2-Point Field Goals',
    '2PA': '2-Point Field Goal Attempts',
    '2P%': '2-Point Field Goal Percentage',
    'eFG%': 'Effective Field Goal Percentage',
    'FT': 'Free Throws',
    'FTA': 'Free Throw Attempts',
    'FT%': 'Free Throw Percentage',
    'ORB': 'Offensive Rebounds',
    'DRB': 'Defensive Rebounds',
    'TRB': 'Total Rebounds',
    'AST': 'Assists',
    'STL': 'Steals',
    'BLK': 'Blocks',
    'TOV': 'Turnovers',
    'PF': 'Personal Fouls',
    'PTS': 'Points'}


def make_hashcode(values):
    """Generate a SHA1 based on the given arguments.
    :param values: value list to generate hash from.
    """

    content = ':'.join(values)
    hashcode = hashlib.sha1(content.encode('utf-8'))
    return hashcode.hexdigest()


def write(es_conn, items):
    """Write items into ElasticSearch.
    :param es_conn: Elasticsearch connection.
    :param items: list of dict objects to be written.
    """

    docs = []
    for item in items:
        # item_id = make_hashcode(list(item.values()))
        item_id = item['csv_id']
        doc = {
            '_index': SEASON_STATS,
            '_id': item_id,
            '_source': item
        }
        docs.append(doc)
    # TODO exception and error handling
    chunk_size = 2000
    chunks = [docs[i:i + chunk_size] for i in range(0, len(docs), chunk_size)]
    for chunk in chunks:
        helpers.bulk(es_conn, chunk)
        print("Chunk uploaded " + str(len(chunk)))
    print("Written: " + str(len(docs)))


def main():
    scripts_dir = os.path.dirname(os.path.realpath('__file__'))
    csv_path = os.path.join(scripts_dir, '../data/nba-players-stats/Seasons_Stats.csv')
    mapping_path = os.path.join(scripts_dir, '../mappings/season_stats.json')

    es_conn = Elasticsearch(['http://localhost:9200'], retry_on_timeout=True, timeout=100, verify_certs=False)

    es_conn.indices.delete(SEASON_STATS, ignore=[400, 404])

    # Read Mapping
    with open(mapping_path) as f:
        mapping = f.read()
    es_conn.indices.create(SEASON_STATS, body=mapping)

    items = []
    with open(csv_path) as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            # Add csv_id column with title instead of untitled first column
            row['csv_id'] = row['']
            del row['']

            # Remove weird blank columns
            del row['blanl']
            del row['blank2']

            # Remove fields with no value
            to_remove = []
            for key in row.keys():
                if not row[key]:
                    to_remove.append(key)
            for key in to_remove:
                del row[key]

            items.append(row)

    write(es_conn, items)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stdout.write(s)
        sys.exit(0)
    except RuntimeError as e:
        s = "Error: %s\n" % str(e)
        sys.stderr.write(s)
        sys.exit(1)
