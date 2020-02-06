import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import logging
from datetime import datetime, timedelta
import json

logging.basicConfig(level=logging.DEBUG)


class FanGraphs(object):
    urls = {
        'leaders': 'https://www.fangraphs.com/leaders.aspx?{}',
        'milb_stats': 'https://www.fangraphs.com/api/leaders/minor-league/data?{}'
    }

    def __init__(self):
        pass

    def __get_leaders_html(self, stats, start_season, end_season=None, league='all', qual=1, ind=1, **kwargs):
        """

        :param stats: pit or bat
        :param start_season:
        :param end_season:
        :param league:
        :param qual:
        :param ind:
        :return:
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
                      'players': kwargs.get('players', ''),
                      'page': '1_999999999'}

        s = requests.get(
            self.urls['leaders'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).content
        return BeautifulSoup(s, "lxml")

    def __get_leader_table(self, type, start_season, **kwargs):
        soup = self.__get_leaders_html(type, start_season, **kwargs)
        table = soup.find('table', {'class': 'rgMasterTable'})
        __data = []
        __headings = [row.text.strip() for row in table.find_all('th')[1:]]

        if type == 'bat':
            # rename the second occurrence of 'FB%' to 'FB% (Pitch)'
            FBperc_indices = [i for i, j in enumerate(__headings) if j == 'FB%']
            __headings[FBperc_indices[1]] = 'FB% (Pitch)'

            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                __data.append([ele for ele in cols[1:]])
        elif type == 'pit':
            __data.append(__headings)
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                __data.append([ele for ele in cols[1:]])

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
        return self.__get_leader_table(type='pit', start_season=start_season, end_season=end_season,
                                       league=league, qual=qual, ind=ind)

    def get_batting_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. Try pitching_leaders(season) or "
                "pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(type='bat', start_season=start_season, end_season=end_season,
                                       league=league, qual=qual, ind=ind)

    def get_team_batting_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. "
                "Try pitching_leaders(season) or pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(type='bat', start_season=start_season, end_season=end_season,
                                       league=league, qual=qual, ind=ind, team=['0', 'ts'])

    def get_team_pitch_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        if start_season is None:
            raise ValueError(
                "You need to provide at least one season to collect data for. "
                "Try pitching_leaders(season) or pitching_leaders(start_season, end_season).")
        if end_season is None:
            end_season = start_season
        return self.__get_leader_table(type='pit', start_season=start_season, end_season=end_season,
                                       league=league, qual=qual, ind=ind, team=['0', 'ts'])

    def get_milb_pitching_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        parameters = {'pos': 'all',
                      'stats': 'pit',
                      'lg': league,
                      'qual': qual,
                      'type': ','.join(['0', ]),
                      'season': start_season,
                      'season1': end_season,
                      'ind': ind,
                      'org': '',
                      'splitTeam': 'false'}

        json = requests.get(
            url=self.urls['milb_stats'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        df = pd.DataFrame(json)
        df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df

    def get_milb_batting_table(self, start_season, end_season=None, league='all', qual=1, ind=1):
        parameters = {'pos': 'all',
                      'stats': 'pit',
                      'lg': league,
                      'qual': qual,
                      'type': ','.join(['0', ]),
                      'season': start_season,
                      'season1': end_season,
                      'ind': ind,
                      'org': '',
                      'splitTeam': 'false'}

        json = requests.get(
            url=self.urls['milb_stats'].format('&'.join(['{}={}'.format(k, v) for k, v in parameters.items()]))).json()

        df = pd.DataFrame(json)
        df['Name'] = df.Name.apply(lambda x: BeautifulSoup(x, 'lxml').a.contents[0])

        return df
