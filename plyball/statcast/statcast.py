import datetime
import io
import logging
from datetime import timedelta

import numpy as np
import pandas as pd
import requests

from plyball.utils import sanitize_input, split_request


class StatCast(object):
    """
    StatCast Web Scraper
    """

    logger = logging.getLogger('statcast')
    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(levelname)s: %(name)s - %(message)s')
    logger.addHandler(c_handler)

    urls = {
        'search': 'https://baseballsavant.mlb.com/statcast_search/csv?{}',
        'game': 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&game_pk={}',
        'milb': 'https://www.mlb.com/prospects/stats/search/csv?{}'
    }

    default_parameters = {
        'all': 'true',
        'hfPT': 'FT',  # Pitch Type
        'hfAB': '',  # Plate Appearance
        'hfBBT': '',  # Batted Ball Type
        'hfPR': '',  # Pitch Result
        'hfZ': '',  # Gameday Zones
        'stadium': '',  # Stadium
        'hfBBL': '',  # Batted Ball Location
        'hfNewZones': '',  # Attack Zones
        'hfGT': 'R|',  # Season Type
        'hfC': '',  # Count
        'hfSea': '2019|',  # Season
        'hfSit': '',  # Situation
        'player_type': 'batter',  # PLayer Type
        'hfOuts': '',  # Outs
        'opponent': '',  # Opponent
        'pitcher_throws': '',  # Pitcher Handedness
        'batter_stands': '',  # Batter Handedness
        'hfSA': '',  # Quality of Contact
        'game_date_gt': '',
        'game_date_lt': '',
        'hfInfield': '',  # Inflield Alignment
        'team': '',
        'position': '',
        'hfOutfield': '',  # Outfield Alignment
        'hfRO': '',  # Runners On
        'home_road': '',
        'hfFlag': '',
        'hfPull': '',  # Batted Ball Direction
        'metric_1': '',
        'hfInn': '',
        'min_pitches': '0',
        'min_results': '0',
        'group_by': 'name',
        'sort_col': 'pitches',
        'player_event_sort': 'h_launch_speed',
        'sort_order': 'desc',
        'min_pas': 0,
        # 'type': 'details&'
    }

    @staticmethod
    def __postprocessing(data, team):
        """
        Scrub Data for Reply

        :param data: StatCast Data to Process
        :param team: Team to Select
        :return: DataFrame
        """
        data.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        data.replace(r'^null$', np.nan, regex=True, inplace=True)

        valid_teams = ['MIN', 'PHI', 'BAL', 'NYY', 'LAD', 'OAK', 'SEA', 'TB', 'MIL', 'MIA',
                       'KC', 'TEX', 'CHC', 'ATL', 'COL', 'HOU', 'CIN', 'LAA', 'DET', 'TOR',
                       'PIT', 'NYM', 'CLE', 'CWS', 'STL', 'WSH', 'SF', 'SD', 'BOS', 'ARI', 'ANA', 'WAS']

        if team in valid_teams:
            data = data.loc[(data['home_team'] == team) | (data['away_team'] == team)]
        elif team is not None:
            raise ValueError('Error: invalid team abbreviation. Valid team names are: {}'.format(valid_teams))
        data = data.reset_index()
        return data

    def __single_game_request(self, game_pk):
        """
        Select Single game for Statcast
        :param game_pk:
        :return:
        """
        url = self.urls['game'].format(game_pk)
        s = requests.get(url, timeout=None).content
        data = pd.read_csv(io.StringIO(s.decode('utf-8')))
        return data

    def statcast_request(self, url: str, headers: bool = False, encoding: str = None):
        """
        Get Information from StatCast

        :param url: Url TO re
        :type url: str
        :param headers:
        :type headers: str
        :param encoding:
        :type encoding: str
        :return: DataFrame
        """

        self.logger.info(url)
        if headers:
            s = requests.get(url, timeout=None, headers=headers).content
        else:
            s = requests.get(url, timeout=None).content

        self.logger.info(s)
        if encoding:
            data = pd.read_csv(io.StringIO(s.decode(encoding)))
        else:
            data = pd.read_csv(io.StringIO(s))
        return data

    def large_request(self,
                      start_date: datetime.date,
                      end_date: datetime.date,
                      step: int,
                      parameters: dict,
                      url: str,
                      verbose: bool):
        """
        Break start and end date into smaller increments, collecting all data in small chunks and appending all
        results to a common dataframe.

        :param url:
        :param parameters:
        :param start_date:
        :param end_date:
        :param step:
        :param verbose:
        :return: pd.DataFrame
        """

        error_counter = 0  # count failed requests. If > X, break
        self.logger.info("This is a large query, it may take a moment to complete")
        dataframe_list = []
        intermediate_dt = start_date + timedelta(days=step)
        while intermediate_dt <= end_date:
            if (intermediate_dt.month < 4 and intermediate_dt.day < 15) or (
                    start_date.month > 10 and start_date.day > 14):
                if end_date.year > intermediate_dt.year:
                    self.logger.info('Skipping Off Season Dates')
                    start_date = start_date.replace(month=3, day=15, year=start_date.year + 1)
                    intermediate_dt = start_date + timedelta(days=step + 1)
                else:
                    break

            parameters['start_date'] = start_date.strftime('%Y-%m-%d')
            parameters['end_date'] = intermediate_dt.strftime('%Y-%m-%d')
            data = self.statcast_request(url.format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
            if 'error' in data.columns:
                success = 0
                while success == 0:
                    parameters['start_date'] = start_date.strftime('%Y-%m-%d')
                    parameters['end_date'] = intermediate_dt.strftime('%Y-%m-%d')
                    data = self.statcast_request(url.format(
                        '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
                    if data.shape[0] > 1:
                        dataframe_list.append(data)
                        success = 1
                    else:
                        error_counter += 1
                    if error_counter > 2:
                        intermediate_dt = intermediate_dt - timedelta(days=1)
                        parameters['start_date'] = start_date.strftime('%Y-%m-%d')
                        parameters['end_date'] = intermediate_dt.strftime('%Y-%m-%d')
                        smaller_data_1 = self.statcast_request(url.format(
                            '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
                        if 'error' in data.columns:
                            self.logger.warning(
                                "Query unsuccessful for data from {} to {}. Skipping these dates.".format(
                                    start_date.strftime('%Y-%m-%d'),
                                    intermediate_dt.strftime('%Y-%m-%d')))
                        else:
                            dataframe_list.append(smaller_data_1)
                        error_counter = 0
                        break
            else:
                dataframe_list.append(data)
                if verbose:
                    self.logger.info(
                        "Completed sub-query from {} to {}".format(start_date.strftime('%Y-%m-%d'),
                                                                   intermediate_dt.strftime('%Y-%m-%d')))
            start_date = intermediate_dt + timedelta(days=1)
            intermediate_dt = intermediate_dt + timedelta(days=step + 1)
        if start_date < end_date:
            parameters['start_date'] = start_date.strftime('%Y-%m-%d')
            parameters['end_date'] = end_date.strftime('%Y-%m-%d')
            data = self.statcast_request(url.format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
            dataframe_list.append(data)
            if verbose:
                logging.info(
                    "Completed sub-query from {} to {}".format(start_date.strftime('%Y-%m-%d'),
                                                               end_date.strftime('%Y-%m-%d')))

        final_data = pd.concat(dataframe_list, axis=0)
        return final_data

    def get_statcast_data(self,
                          start_dt: datetime.date = None,
                          end_dt: datetime.date = None,
                          team: str = None,
                          detail: bool = True,
                          verbose: bool = True) -> pd.DataFrame:
        """
        Pulls Statcast play-level data from Baseball Savant for a given date range. If no arguments are provided, this
        will return yesterday's statcast data. If one date is provided, it will return that date's Statcast data.

        :param start_dt: First date for which you want StatCast data
        :type start_dt: datetime.date
        :param end_dt: Last date for which you want StatCast data
        :type end_dt: datetime.date
        :param team: City abbreviation of the team you want data for (e.g. SEA or BOS)
        :type team: str
        :param detail: Provide Play-Level Detail
        :type detail: bool
        :param verbose:
        :type verbose: bool
        :return: DataFrame
        """

        parameters = self.default_parameters

        if detail:
            parameters['type'] = 'details&'

        small_query_threshold = 5
        data = self.large_request(start_dt,
                                  end_dt,
                                  step=small_query_threshold,
                                  parameters=parameters,
                                  url=self.urls['search'],
                                  verbose=verbose)

        data = self.__postprocessing(data, team)
        return data

    def single_game(self, game_pk: str, team: str = None) -> pd.DataFrame:
        """
        Pulls statcast play-level data from Baseball Savant for a single game, identified by its MLB game ID (game_pk
        in statcast data)

        :param self:
        :param game_pk: 6-digit integer MLB game ID to retrieve
        :type game_pk: str
        :param team:
        :type team: str
        :return: DataFrame
        """
        data = self.__single_game_request(game_pk)
        data = self.__postprocessing(data, team)
        return data

    @staticmethod
    def batter(start_dt: datetime.date, end_dt: datetime.date, player_id: str) -> pd.DataFrame:
        """
        Pulls statcast batter-level data from Baseball Savant for a given batter.

        :param start_dt: the first date for which you want data
        :type start_dt: datetime.date
        :param end_dt: the final date for which you want data
        :type end_dt: datetime.date
        :param player_id: the player's MLBAM ID. Find this by calling :func:plyball.player_id_lookup.
        :type player_id: str
        :return: DataFrame of StatCast Data for Batters
        :rtype: DataFrame
        """

        start_dt, end_dt, player_id = sanitize_input(start_dt, end_dt, player_id)
        url = ('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium'
               '=&hfBBL=&hfNewZones=&hfGT=R|PO|S|=&hfSea=&hfSit=&player_type=batter&hfOuts=&opponent'
               '=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&batters_lookup%5B%5D={'
               '}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by'
               '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details& ')
        df = split_request(start_dt, end_dt, player_id, url)
        return df

    @staticmethod
    def pitcher(start_dt: datetime.date, end_dt: datetime.date, player_id: str)-> pd.DataFrame:
        """
        Pulls statcast pitch-level data from Baseball Savant for a given pitcher.

        :param start_dt: the first_name date for which you want a player's sSatcast data
        :param end_dt: the final date for which you want data
        :param player_id: the player's MLBAM ID.
        :return: Dataframe of StatCast Data for Pitchers
        """

        start_dt, end_dt, player_id = sanitize_input(start_dt, end_dt, player_id)

        url = ('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium'
               '=&hfBBL=&hfNewZones=&hfGT=R|PO|S|=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent'
               '=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&pitchers_lookup[]={'
               '}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by'
               '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details& ')
        df = split_request(start_dt, end_dt, player_id, url)
        return df

    def milb_statcast(self,
                      start_date: datetime.date,
                      end_date: datetime.date,
                      details: bool,
                      verbose: bool,
                      **kwargs) -> pd.DataFrame:
        """
        Get StatCast Data for MILB players

        :param start_date:  the first date for which you want data

        :param end_date:  the last date for which you want data
        :param details: Aggregate Stats
        :param verbose: Increased Logging
        :return: Dataframe of MILB players
        :rtype: pd.DataFrame
        """
        small_query_threshold = 5
        parameters = {
            'pitcher_throws': '',
            'game_date_gt': '{}'.format(start_date.strftime('%Y-%m-%d')),
            'game_date_lt': '{}'.format(end_date.strftime('%Y-%m-%d')),
            'season': '',
            'home_away': '',
            'draft_year': '',
            'prospect': '',
            'player_type': 'batter',
            'sort_by': 'results',
            'sort_order': 'desc',
            'group_by': 'name',
            'min_results': '',
            'min_pa': ''
        }

        for k, v in kwargs.items():
            parameters[k] = v

        if details:
            parameters['is_aggregate'] = 'false'

        data = self.large_request(start_date,
                                  end_date,
                                  step=small_query_threshold,
                                  parameters=parameters,
                                  url=self.urls['milb'],
                                  verbose=verbose)

        return data
