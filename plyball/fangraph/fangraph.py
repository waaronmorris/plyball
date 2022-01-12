import logging
import re
from typing import Literal

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


class FanGraphs(object):
    """
    Class for scraping data from FanGraphs

    """
    _urls = {
        'leaders': 'https://www.fangraphs.com/leaders.aspx?{}',
        'milb_stats': 'https://www.fangraphs.com/api/leaders/minor-league/data?{}'
    }

    logger = logging.getLogger('fangraphs')

    def __init__(self):
        pass

    def __get_leaders_html(self,
                           player_type: Literal['bat', 'pit'],
                           start_season: int,
                           end_season: int = None,
                           league: int = 'all',
                           qualified: int = 'y',
                           split_season: int = 1,
                           **kwargs):
        """
        Get Leaderboard HTML form Fangraphs to convert into a DataFrame

        :param player_type: pit or bat
        :type player_type: Literal['bat','pit']
        :param start_season:
        :param end_season:
        :param league:
        :param qualified: The minimum pla
        :param split_season:
        :return: BeautifulSoup
        """

        if player_type == 'pit':
            columns = ['c'] + [str(num) for num in range(3, 322)]
        elif player_type == 'bat':
            columns = ['c'] + [str(num) for num in range(3, 305)]
        else:
            columns = ['c']

        parameters = {'pos': 'all',
                      'stats': player_type,
                      'lg': league,
                      'qual': qualified,
                      'type': ','.join(columns),
                      'season': start_season,
                      'month': 0,
                      'season1': end_season,
                      'ind': split_season,
                      'team': ','.join(kwargs.get('team', [''])),
                      'rost': kwargs.get('rost', ''),
                      'age': kwargs.get('age', ''),
                      'filter': kwargs.get('filter', ''),
                      'players': kwargs.get('players', '0'),
                      'page': '1_999999999'}

        self.logger.info(self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
        s = requests.get(
            self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).content
        self.logger.info(self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
        return BeautifulSoup(s, "lxml")

    def __get_leader_table(self,
                           player_type: Literal['pit', 'bat'],
                           start_season: int,
                           **kwargs) -> pd.DataFrame:

        soup = self.__get_leaders_html(player_type, start_season, **kwargs)
        table = soup.find('table', {'class': 'rgMasterTable'})

        __data = []
        __headings = [row.text.strip() for row in table.find_all('th')[1:]]
        __headings.append('player_id')

        fb_perc_indices = [i for i, j in enumerate(__headings) if j == 'FB%']
        __headings[fb_perc_indices[0]] = 'flyball_%'
        __headings[fb_perc_indices[1]] = 'fastball_%'
        __data.append(__headings)
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')

        for row in rows:
            player_id = 'NA'
            cols = row.find_all('td')
            table_cols = []
            for ele in cols:
                table_cols.append(ele.text.strip())
                for link in ele.find_all('a'):
                    url = link.get('href', None)
                    if 'playerid' in url:
                        player_id = url.split('?')[1].split('&')[0].split('=')[1]
            table_cols.append(player_id)
            __data.append([ele for ele in table_cols[1:]])

        __data = pd.DataFrame(data=__data, columns=__headings)[1:]

        # replace empty strings with NaN
        __data.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        # convert percentage strings to floats   FB% duplicated
        percentages = [column for column in __data.columns if '%' in column]
        for col in percentages:
            if not __data[col].empty:
                if pd.api.types.is_string_dtype(__data[col]):
                    __data[col] = __data[col].str.strip(' %')
                    __data[col] = __data[col].str.strip('%')
                    __data[col] = pd.to_numeric(__data[col], errors='coerce') / 100.

        __data.columns = [
            re.sub(r"\W", '_',
                   column.replace('%', '_percent').replace('-', '_negative_').replace('+', '_positive_').replace('__',
                                                                                                                 '_'))
            for column in __data.columns]
        __data['player_type'] = player_type
        return __data

    def get_pitching_table(self,
                           start_season: int,
                           end_season: int = None,
                           league: str = 'all',
                           minimum_innings_pitched: int = None,
                           split_season: bool = True):
        """
        Get individual pitching data from LeaderBoard.

        :param start_season:
        :param end_season:
        :param league:
        :param minimum_innings_pitched:
        :param split_season:
        :return:
        """
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. Try pitching_leaders(season) or "
                "pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(player_type='pit',
                                       start_season=start_season,
                                       end_season=end_season,
                                       league=league,
                                       qualified='y' if minimum_innings_pitched is None else minimum_innings_pitched,
                                       ind=1 if split_season else 0)

    def get_batting_table(self,
                          start_season,
                          end_season: int = None,
                          league: str = 'all',
                          minimum_plate_appearances: int = None,
                          split_season: bool = True):
        """
        Get Individual Batting Stats from the LeaderBoard.

        :param start_season:
        :param end_season:
        :param league:
        :param minimum_plate_appearances:
        :param split_season:
        :return:
        """
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. Try pitching_leaders(season) or "
                "pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(player_type='bat',
                                       start_season=start_season,
                                       end_season=end_season,
                                       league=league,
                                       qualified='y' if minimum_plate_appearances is None else minimum_plate_appearances,
                                       ind=1 if split_season else 0)

    def get_team_batting_table(self,
                               start_season: int,
                               end_season: int = None,
                               league: str = 'all',
                               minimum_plate_appearances: int = None,
                               split_season: bool = True):
        """
        Get Team Batting Stats from the LeaderBoard.

        :param start_season: First Season to return data
        :param end_season: Last Season to return data
        :param league: American (AL) or National (NL) Baseball League
        :param minimum_plate_appearances: Minimum number of Plate Appearances (Not supplied only minimum_innings_pitched players will be returned)
        :param split_season: Return Season Splits for players
        :return:
        """
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. "
                "Try pitching_leaders(season) or pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(player_type='bat',
                                       start_season=start_season,
                                       end_season=end_season,
                                       league=league,
                                       qualified='y' if minimum_plate_appearances is None else minimum_plate_appearances,
                                       ind=1 if split_season else 0,
                                       team=['0', 'to'])

    def get_team_pitch_table(self,
                             start_season: int,
                             end_season: int = None,
                             league: str = 'all',
                             minimum_innings_pitched: int = None,
                             split_season: bool = True):
        """
        Get Team Pitching Stats from teh LeaderBoard.


        :param start_season: First Season to return data
        :param end_season: Last Season to return data
        :param league: American (AL) or National (NL) Baseball League
        :param minimum_innings_pitched: Minimum number of Pitching Appearances (Not supplied only minimum_plate_appearances players will be returned)
        :param split_season: Return Season Splits for players

        :return: DataFrame
        """
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. "
                "Try pitching_leaders(season) or pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(player_type='pit',
                                       start_season=start_season,
                                       end_season=end_season,
                                       league=league,
                                       qualified='y' if minimum_innings_pitched is None else minimum_innings_pitched,
                                       ind=1 if split_season else 0,
                                       team=['0', 'to'])

    def get_milb_pitching_table(self,
                                start_season: int,
                                end_season: int = None,
                                league: str = 'all',
                                minimum_innings_pitched: int = None,
                                split_season: int = 1):
        """
        Get Minor League Pitching Stats from the Prospects sections

        :param start_season: First Season to return data
        :param end_season: Last Season to return data
        :param league: American (AL) or National (NL) Baseball League
        :param minimum_innings_pitched: Minimum number of Plate Appearances (Not supplied only minimum_innings_pitched players will be returned)
        :param split_season: Return Season Splits for players
        :return: DataFrame
        """
        parameters = {'pos': 'all',
                      'stat': 'pit',
                      'lg': league,
                      'qual': 'y' if minimum_innings_pitched is None else minimum_innings_pitched,
                      'player_type': ','.join(['0', ]),
                      'season': start_season,
                      'season1': end_season,
                      'ind': split_season,
                      'org': '',
                      'splitTeam': 'false'}

        json = requests.get(
            url=self._urls['milb_stats'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        df = pd.DataFrame(json)
        df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df

    def get_milb_batting_table(self,
                               start_season: int,
                               end_season: int = None,
                               league: str = 'all',
                               minimum_plate_appearances: int = None,
                               split_season: bool = True) -> pd.DataFrame:
        """
        Get Minor League Pitching from the Prospects sections.

        :param start_season: First Season to return data
        :param end_season: Last Season to return data
        :param league: American (AL) or National (NL) Baseball League
        :param minimum_plate_appearances: Minimum number of Plate Appearances (Not supplied only minimum_innings_pitched players will be returned)
        :param split_season: Return Season Splits for players
        :return: DataFrame
        """
        parameters = {'pos': 'all',
                      'stats': 'pit',
                      'lg': league,
                      'qual': 'y' if minimum_plate_appearances is None else minimum_plate_appearances,
                      'player_type': ','.join(['0', ]),
                      'season': start_season,
                      'season1': end_season,
                      'ind': 1 if split_season else 0,
                      'org': '',
                      'splitTeam': 'false'}

        json = requests.get(
            url=self._urls['milb_stats'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        df = pd.DataFrame(json)
        df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df
