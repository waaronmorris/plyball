import pandas as pd

from plyball.data.web import fangraph
from plyball.data.web.fangraph import Ottoneu


class Ottoneu(object):
    """
    Consolidated Fantasy Related Data for Ottoneu

    """
    scoring = {
        'batting': {'AB': -1.0,
                    'H': 5.6,
                    '2B': 2.9,
                    '3B': 5.7,
                    'HR': 9.4,
                    'BB': 3.0,
                    'HBP': 3.0,
                    'SB': 1.9,
                    'CS': -2.8},
        'pitching': {'IP': 5.0,
                     'SO': 2.0,
                     'BB': -3.0,
                     'HBP': -3.0,
                     'HR': -13.0,
                     'SV': 5.0,
                     'HLD': 4.0,
                     }
    }

    def __init__(self, league_id):
        self.__ottoneu = fangraph.Ottoneu(league_id)
        self.__fangraph = fangraph.FanGraphs()

    def __ottoneu_data(self):
        data = self.__ottoneu.players()


    def __calculate_fangraph_sabr_points(self, fangraph_stats, position_type):
        """

        :param fangraph_stats: DataFrame to calculate SABR points
        :param position_type: `batting` or `pitching`
        :return: Panda Series of SABR Points
        """
        fangraph_stats['fantasy_points'] = 0
        if position_type == 'pitching':
            fangraph_stats['IP'] = pd.to_numeric(fangraph_stats['IP']).round(0) + (pd.to_numeric(fangraph_stats['IP'])
                                                                                   - pd.to_numeric(fangraph_stats['IP']).round(0)[
                                                                           1:]) * 3.33
            for stat, points in self.scoring['pitching'].items():
                fangraph_stats['fantasy_points'] = (fangraph_stats['fantasy_points'] +
                                                    (pd.to_numeric(fangraph_stats[stat], errors='coerce') * points))

        elif position_type == 'batting':
            for stat, points in self.scoring['batting'].items():
                fangraph_stats['fantasy_points'] = (fangraph_stats['fantasy_points'] +
                                                    (pd.to_numeric(fangraph_stats[stat], errors='coerce') * points))

        return fangraph_stats['fantasy_points']

    def _fangraph_data(self, position_type, year):
        """
        Get Fantasy Related Data from FanGraphs.

        :param position_type: 'pitching' or 'batting'
        :return: Pitchers DataFrame, Batters DataFrame
        """
        rv = self.__fangraph.get_batting_table(year)
        rv['SABR_points'] = self.__calculate_fangraph_SABR(rv, position_type)
        return rv




