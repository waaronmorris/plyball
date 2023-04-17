import pandas as pd

from plyball.player_map import get_player_map
from plyball.fangraphs import FanGraphs
from plyball.ottoneu import Ottoneu


def calculate_sabr_points(fangraph_stats, position_type):
    """
    Function for calculating number of points in SABR fantasy leagues.
    TODO: make function for more generic leagues

    :param fangraph_stats:
    :param position_type:
    :return:
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

    fangraph_stats['fantasy_points'] = 0
    if position_type == 'pitching':
        fangraph_stats['IP'] = pd.to_numeric(fangraph_stats['IP']).round(0) \
                               + (pd.to_numeric(fangraph_stats['IP'])
                                  - pd.to_numeric(fangraph_stats['IP']).round(0)[1:]) \
                               * 3.33
        for stat, points in scoring['pitching'].items():
            fangraph_stats['fantasy_points'] = (fangraph_stats['fantasy_points'] +
                                                (pd.to_numeric(fangraph_stats[stat], errors='coerce') * points))

    elif position_type == 'batting':
        for stat, points in scoring['batting'].items():
            fangraph_stats['fantasy_points'] = (fangraph_stats['fantasy_points'] +
                                                (pd.to_numeric(fangraph_stats[stat], errors='coerce') * points))

    return fangraph_stats['fantasy_points']


def player_yearly_summary_data():
    """

    :return:
    """
    __player_map = get_player_map()
    __ottoneu = Ottoneu(186).players()['info']
    __ottoneu['PlayerID'] = pd.to_numeric(__ottoneu['PlayerID'], errors='coerce')
    __ottoneu = __ottoneu.merge(__player_map,
                                right_on='OTTONEUID',
                                left_on='PlayerID')

    __fangraph_pitching = FanGraphs().get_pitching_table(2019)
    __fangraph_pitching['fantasy_points'] = calculate_sabr_points(__fangraph_pitching, 'pitch')
    __fangraph_batting = FanGraphs().get_batting_table(2019)
    __fangraph_batting['fantasy_points'] = calculate_sabr_points(__fangraph_batting, 'batting')

    __fangraph = pd.merge(__fangraph_pitching,
                          __fangraph_batting,
                          on='player_id',
                          how='outer', suffixes=('_pitch', '_bat'))

    df = __ottoneu.merge(__fangraph, right_on='player_id', left_on='IDFANGRAPHS', how='left')
    df.Points = df.Points.fillna(df.fantasy_points_bat.fillna(0) + df.fantasy_points_pitch.fillna(0))

    return df
