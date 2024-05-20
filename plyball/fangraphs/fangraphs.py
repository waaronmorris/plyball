import logging
import re
import warnings
from typing import Literal

import numpy as np
import pandas as pd
import requests
import structlog
from bs4 import BeautifulSoup


class FanGraphs(object):
    """
    Class for scraping data from FanGraphs

    """
    _urls = {
        'leaders': 'https://www.fangraphs.com/api/leaders/major-league/data?{}',
        'milb_stats': 'https://www.fangraphs.com/api/leaders/minor-league/data?{}',
        'projections': 'https://www.fangraphs.com/api/projections?{}'
    }

    logger = structlog.get_logger("FanGraphs")

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

        parameters = {
            'pos': 'all',
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
            'page': '1_999999999',
            'pageitems': '2000000000'
        }

        s = requests.get(
            self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()
        self.logger.info(self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
        return s

    def __get_leader_table(self,
                           player_type: Literal['pit', 'bat'],
                           start_season: int,
                           **kwargs) -> pd.DataFrame:

        data = self.__get_leaders_html(player_type, start_season, **kwargs)['data']

        __data = pd.DataFrame(data=data)

        self.logger.info("Loaded DataFrame with {} rows".format(__data.shape[0]),
                         shape=__data.shape,
                         example=__data.head(1))

        # replace empty strings with NaN
        __data.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        # convert percentage strings to floats FB% duplicated
        percentages = [column for column in __data.columns if '%' in column]
        for col in percentages:
            if not __data[col].empty:
                if pd.api.types.is_string_dtype(__data[col]):
                    __data[col] = __data[col].str.strip(' %')
                    __data[col] = __data[col].str.strip('%')
                    __data[col] = pd.to_numeric(__data[col], errors='coerce') / 100.

        __data.columns = [
            re.sub(r"\W", '_',
                   column.replace('%', '_percent').replace('-', '_negative_').replace('+', '_positive_').replace(
                       '__',
                       '_'))
            for column in __data.columns]

        with warnings.catch_warnings():
            """
            Pandas will throw a performance warning due to fragmentation of the data. This is expected behavior and
            can be ignored. The DataFrame is copied to avoid this warning from being thrown for the user.
            """
            warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
            __data['player_type'] = player_type

        return __data.copy()

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
                                       qualified='0' if minimum_innings_pitched is None else minimum_innings_pitched,
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
                                       qualified='0' if minimum_plate_appearances is None else minimum_plate_appearances,
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
                                split_season: int = 0):
        """
        Get Minor League Pitching Stats from the Prospects sections

        :param start_season: First Season to return data
        :param end_season: Last Season to return data
        :param league: American (AL) or National (NL) Baseball League
        :param minimum_innings_pitched: Minimum number of Plate Appearances (Not supplied only minimum_innings_pitched players will be returned)
        :param split_season: Return Season Splits for players
        :return: DataFrame
        """
        parameters = {
            'pos': 'all',
            'level': 0,
            'stats': 'pit',
            'lg': league,
            'qual': '0' if minimum_innings_pitched is None else minimum_innings_pitched,
            'type': '0',
            'team': '',
            'season': start_season,
            'seasonEnd': start_season if end_season is None else end_season,
            'ind': split_season,
            'org': '',
            'splitTeam': 'false'
        }
        # pos = all & level = 0 & lg = 2 & stats = bat & qual = y & type = 0 & team = & season = 2023 & seasonEnd = 2023 & org = & ind = 0 & splitTeam = false

        json = requests.get(
            url=self._urls['milb_stats'].format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

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
        parameters = {
            'pos': 'all',
            'level': 0,
            'lg': league,
            'stats': 'bat',
            'qual': '0' if minimum_plate_appearances is None else minimum_plate_appearances,
            'team': '',
            'type': '0',
            'season': start_season,
            'seasonEnd': start_season if end_season is None else end_season,
            'org': '',
            'ind': 1 if split_season else 0,
            'splitTeam': 'false'
        }

        # ?pos = all & level = 0 & lg = all & stats = pit & qual = 0 & type = 0 & team = & season = 2023 & seasonEnd = 2023 & org = & ind = 0 & splitTeam = false & players = & sort = 25, 1

        json = requests.get(
            url=self._urls['milb_stats'].format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        self.logger.info("JSON", json=json)
        df = pd.DataFrame(json)
        # df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df

    def get_zip_bat_projections(self) -> pd.DataFrame:
        parameters = {
            'type': 'steamer',
            'stats': 'bat',
            'pos': 'all',
            'team': '0',
            'players': '0',
            'lg': 'all',
        }

        json = requests.get(
            url=self._urls['projections'].format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()
        df = pd.DataFrame(json)

        return df

    def get_zip_pitching_projections(self) -> pd.DataFrame:
        parameters = {
            'type': 'steamer',
            'stats': 'pit',
            'pos': 'all',
            'team': '0',
            'players': '0',
            'lg': 'all',
        }

        json = requests.get(
            url=self._urls['projections'].format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()
        df = pd.DataFrame(json)

        return df

    def get_daily_game_log(
            self,
            player_id: int,
            position: str,
            season: int = None,
            start_date: str = None,
            end_date: str = None,
            **kwargs):
        """
        https://www.fangraphs.com/api/players/game-log?playerid=10155&position=OF&type=0&season=2023
        :return:
        """

        parameters = {
            'playerid': player_id,
            'position': position,
            'type': 0,
            'season': season,
            'start': start_date,
            'end': end_date
        }

        url = 'https://www.fangraphs.com/api/players/game-log?{}'.format(
            '&'.join(['{}={}'.format(k, v) for k, v in parameters.items() if v != '' and v is not None]))

        logging.info(url)

        json = requests.get(url).json()

        df = pd.DataFrame(json['mlb'])

        return df
