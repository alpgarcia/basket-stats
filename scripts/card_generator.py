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
import argparse
import csv
import sys

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q


class PlayerCard:
    """Represents a single player card.

    """

    NAME = 'Name'
    YEAR = 'Year'
    TEAM = 'Team'
    POS = 'Position'
    TWO_P = '2PT'
    THREE_P = '3PT'
    DRB = 'DRB'
    ORB = 'ORB'
    AST = 'AST'
    BLK = 'BLK'
    STL = 'STL'
    SKL = 'SKL'

    def __init__(self, name, year, team, pos, two_p, three_p, drb, orb, ast, blk, stl, skl):
        self.__name = name
        self.__year = year
        self.__team = team
        self.__pos = pos
        self.__two_p = two_p
        self.__three_p = three_p
        self.__drb = drb
        self.__orb = orb
        self.__ast = ast
        self.__blk = blk
        self.__stl = stl
        self.__skl = skl

    def name(self):
        return self.__name

    def year(self):
        return self.__year

    def team(self):
        return self.__team

    def pos(self):
        return self.__pos

    def two_p(self):
        return self.__two_p

    def three_p(self):
        return self.__three_p

    def drb(self):
        return self.__drb

    def orb(self):
        return self.__orb

    def ast(self):
        return self.__ast

    def blk(self):
        return self.__blk

    def stl(self):
        return self.__stl

    def tov(self):
        return self.__skl

    def to_dict(self):
        return {
            self.NAME: self.__name,
            self.YEAR: self.__year,
            self.TEAM: self.__team,
            self.POS: self.__pos,
            self.TWO_P: self.__two_p,
            self.THREE_P: self.__three_p,
            self.DRB: self.__drb,
            self.ORB: self.__orb,
            self.AST: self.__ast,
            self.BLK: self.__blk,
            self.STL: self.__stl,
            self.SKL: self.__skl

        }

    def __str__(self):
        return "%s, %s, %s, %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s" % \
               (self.__name, self.__year, self.__team, self.__pos,
                self.TWO_P, self.__two_p,
                self.THREE_P, self.__three_p,
                self.DRB, self.__drb,
                self.ORB, self.__orb,
                self.AST, self.__ast,
                self.BLK, self.__blk,
                self.STL, self.__stl,
                self.SKL, self.__skl)


class CardGenerator:
    """Generates cards from stats stored in ElasticSearch.

    """
    SEASON_STATS = 'season_stats'

    TWO_P = '2P%'
    THREE_P = '3P%'
    DRB = 'DRB%'
    ORB = 'ORB%'
    AST = 'AST%'
    BLK = 'BLK%'
    STL = 'STL%'
    TOV = 'TOV%'

    def __init__(self, es_conn):
        self.__es_conn = es_conn

    @staticmethod
    def __calc_attr(thresholds, value):
        attr_value = 1
        if value > thresholds['90.0']:
            attr_value = 4
        elif value > thresholds['50.0']:
            attr_value = 3
        elif value > thresholds['20.0']:
            attr_value = 2

        return attr_value

    @staticmethod
    def __calc_skl_attr(thresholds, tov_value):
        attr_value = 4
        if tov_value >= thresholds['80.0']:
            attr_value = 1
        elif tov_value >= thresholds['50.0']:
            attr_value = 2
        elif tov_value >= thresholds['10.0']:
            attr_value = 3

        return attr_value

    def __percentiles(self, field, percents, filter_q=None):
        """

        :param field:
        :param percents:
        :param filter_q: filter query to exclude or include items in the calculation.
        :return: dict with entries for 20, 50 and 90 percentiles.
        """
        search = Search(using=self.__es_conn)
        search = search[0:0]
        if filter_q:
            search = search.filter(filter_q)
        search = search.aggs.metric('percentiles', 'percentiles', field=field, percents=percents)

        response = search.execute()
        values = response.to_dict()['aggregations']['percentiles']['values']

        return values

    def __calculate_thresholds(self):
        """

        :return: dict with percentiles per each attribute. Each attribute will contain 20, 50 and 90 percentiles.
        """

        thresholds = {}

        percents = [20, 50, 90]

        #  At least 20 2-point field goals attempts
        q = Q('range')
        q.__setattr__('2PA', {'gt': 20})
        thresholds[self.TWO_P] = self.__percentiles(self.TWO_P, percents, q)

        #  At least 20 3-point field goals attempts
        q = Q('range')
        q.__setattr__('3PA', {'gt': 20})
        thresholds[self.THREE_P] = self.__percentiles(self.THREE_P, percents, q)

        thresholds[self.DRB] = self.__percentiles(self.DRB, percents)

        thresholds[self.ORB] = self.__percentiles(self.ORB, percents)

        thresholds[self.AST] = self.__percentiles(self.AST, percents)

        thresholds[self.BLK] = self.__percentiles(self.BLK, percents)

        thresholds[self.STL] = self.__percentiles(self.STL, percents)

        # This is a special case as it is used to calculate skills attribute.
        # SKL could be seen as  (1 - TOV), so basically we want the 10% of players
        # having less turnovers, then the 50% and then the 20% of players having
        # the most turnovers.
        thresholds[self.TOV] = self.__percentiles(self.TOV, [10, 50, 80])

        return thresholds

    def create_player_cards(self, name, year=None):
        """Creates a card based on real stats retrieved from ElasticSearch.

        :param name:
        :param year: if None, cards for all available years will be created.
        :return:
        """

        thresholds = self.__calculate_thresholds()

        search = Search(using=self.__es_conn)
        search = search.filter('term', Player=name)
        if year:
            search = search.filter('term', Year=year)

        for hit in search.scan():
            two_p = self.__calc_attr(thresholds[self.TWO_P], float(hit[self.TWO_P])) if self.TWO_P in hit else 1
            three_p = self.__calc_attr(thresholds[self.THREE_P], float(hit[self.THREE_P])) if self.THREE_P in hit else 1
            drb = self.__calc_attr(thresholds[self.DRB], float(hit[self.DRB])) if self.DRB in hit else 1
            orb = self.__calc_attr(thresholds[self.ORB], float(hit[self.ORB])) if self.ORB in hit else 1
            ast = self.__calc_attr(thresholds[self.AST], float(hit[self.AST])) if self.AST in hit else 1
            blk = self.__calc_attr(thresholds[self.BLK], float(hit[self.BLK])) if self.BLK in hit else 1
            stl = self.__calc_attr(thresholds[self.STL], float(hit[self.STL])) if self.STL in hit else 1
            skl = self.__calc_skl_attr(thresholds[self.TOV], float(hit[self.TOV])) if self.TOV in hit else 1

            player_card = PlayerCard(name=name, year=hit['Year'], team=hit['Tm'], pos=hit['Pos'],
                                     two_p=two_p,
                                     three_p=three_p,
                                     drb=drb,
                                     orb=orb,
                                     ast=ast,
                                     blk=blk,
                                     stl=stl,
                                     skl=skl)
            yield player_card


def read_draft(draft_path):
    """Read player names and years (if any)
    
    :param draft_path: 
    :return: 
    """

    players = []
    with open(draft_path) as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            name = row.get('name')
            year = row.get('year')
            players.append((name, year))

    return players


def write_cards_csv(cards_filepath, player_cards):
    """Write cards to a CSV file

    :param cards_filepath:
    :param player_cards:
    :return:
    """
    with open(cards_filepath, 'w') as csvfile:
        fieldnames = [PlayerCard.NAME, PlayerCard.YEAR, PlayerCard.TEAM, PlayerCard.POS,
                      PlayerCard.TWO_P, PlayerCard.THREE_P, PlayerCard.DRB, PlayerCard.ORB,
                      PlayerCard.AST, PlayerCard.BLK, PlayerCard.STL, PlayerCard.SKL]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for player_card in player_cards:
            writer.writerow(player_card.to_dict())


def main():
    """Creates cards corresponding to a list of player names and years.

    A CSV file with player names and years is requires as input. If year
    is empty, cards for all years will be created.

    Player cards are written in another CSV file.

    """
    parser = argparse.ArgumentParser(description='Card generator')
    parser.add_argument('draft_filepath', type=str, help='Input CSV file')
    parser.add_argument('cards_filepath', type=str, help='Output CSV file')

    args = parser.parse_args()

    es_conn = Elasticsearch(['http://localhost:9200'], retry_on_timeout=True, timeout=100, verify_certs=False)
    card_generator = CardGenerator(es_conn)

    player_cards = []
    for player in read_draft(args.draft_filepath):
        player_cards.extend(card_generator.create_player_cards(player[0], player[1]))

    write_cards_csv(args.cards_filepath, player_cards)


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
