import logging

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

    def __init__(self):
        pass

    def __get_leaders_html(self, stats, start_season, end_season=None, league='all', qual='y', ind=1, **kwargs):
        """
        Get Leadeboard HTML form Fangraphs to convert into a DataFrame

        :param stats: pit or bat
        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
        :return: BeautifulSoup
        """

        if stats == 'pit':
            columns = ['c'] + [str(num) for num in range(3, 322)]
        elif stats == 'bat':
            columns = ['c'] + [str(num) for num in range(3, 305)]
        else:
            columns = ['c']

        parameters = {'pos': 'all',
                      'stats': stats,
                      'lg': league,
                      'qual': qual,
                      'type': ','.join(columns),
                      'season': start_season,
                      'month': 0,
                      'season1': end_season,
                      'ind': ind,
                      'team': ','.join(kwargs.get('team', [''])),
                      'rost': kwargs.get('rost', ''),
                      'age': kwargs.get('age', ''),
                      'filter': kwargs.get('filter', ''),
                      'players': kwargs.get('players', '0'),
                      'page': '1_999999999'}

        s = requests.get(
            self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).content
        logging.info(self._urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
        return BeautifulSoup(s, "lxml")

    def __get_leader_table(self, player_type, start_season, **kwargs):
        soup = self.__get_leaders_html(player_type, start_season, **kwargs)
        table = soup.find('table', {'class': 'rgMasterTable'})

        __data = []
        __headings = [row.text.strip() for row in table.find_all('th')[1:]]
        __headings.append('player_id')

        if player_type == 'bat':
            FBperc_indices = [i for i, j in enumerate(__headings) if j == 'FB%']
            __headings[FBperc_indices[1]] = 'FB% (Pitch)'
        elif player_type == 'pit':
            pass

        __data.append(__headings)
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
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
        return __data

    def get_pitching_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        """
        Get individual pitching data from LeaderBoard.

        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
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
                                       qual=qual,
                                       ind=ind)

    def get_batting_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        """
        Get Individual Batting Stats from the LeaderBoard.

        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
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
                                       qual=qual,
                                       ind=ind)

    def get_team_batting_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        """
        Get Team Batting Stats from the LeaderBoard.

        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
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
                                       qual=qual,
                                       ind=ind,
                                       team=['0', 'ts'])

    def get_team_pitch_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        """
        Get Team Pitching Stats from teh LeaderBoard.

        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
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
                                       qual=qual,
                                       ind=ind,
                                       team=['0', 'ts'])

    def get_milb_pitching_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        """
        Get Minor League Pitching Stats from the Prospects sections

        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
        :return: DataFrame
        """
        parameters = {'pos': 'all',
                      'stats': 'pit',
                      'lg': league,
                      'qual': qual,
                      'player_type': ','.join(['0', ]),
                      'season': start_season,
                      'season1': end_season,
                      'ind': ind,
                      'org': '',
                      'splitTeam': 'false'}

        json = requests.get(
            url=self._urls['milb_stats'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        df = pd.DataFrame(json)
        df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df

    def get_milb_batting_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        """
        Get Minor League Pitching from the Prospects sections.

        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
        :return: DataFrame
        """
        parameters = {'pos': 'all',
                      'stats': 'pit',
                      'lg': league,
                      'qual': qual,
                      'player_type': ','.join(['0', ]),
                      'season': start_season,
                      'season1': end_season,
                      'ind': ind,
                      'org': '',
                      'splitTeam': 'false'}

        json = requests.get(
            url=self._urls['milb_stats'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        df = pd.DataFrame(json)
        df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df
