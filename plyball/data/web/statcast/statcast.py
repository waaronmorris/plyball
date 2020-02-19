import io
import logging
from datetime import timedelta

import numpy as np
import pandas as pd
import requests

from plyball.data.utils import sanitize_input, split_request

logger = logging.getLogger('StatCast')
c_handler = logging.StreamHandler()
c_format = logging.Formatter('%(levelname)s: %(name)s - %(message)s')
logger.addHandler(c_handler)


class StatCast(object):
    """
    StatCast Web Scraper
    """
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

    @staticmethod
    def statcast_request(url, headers=False, encoding=None):
        """

        :param url:
        :return:
        """

        headers = {
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
            'Sec-Fetch-User': '?1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Referer': 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R|&hfC=&hfSea=2019|&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=2019-08-01&game_date_lt=2019-08-31&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfPull=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_pas=0&chk_pitch_type=on&chk_pitch_result=on&chk_bb_type=on&chk_count=on&chk_batter_stands=on&chk_takes=on',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': 'AMCVS_A65F776A5245B01B0A490D44%40AdobeOrg=1; s_ecid=MCMID|76932247013101732222268800691362456112; _gcl_au=1.1.1513098462.1579648486; btIdentify=cfd6af57-74af-4b7d-a816-5ce347301cfb; _bti=%7B%22bsin%22%3A%22%22%7D; s_cc=true; _fbp=fb.1.1579648488603.1129893510; __gads=ID=cb7ecf3d7321b3a0:T=1579648489:S=ALNI_MZZwhO232BTMssupgFaZoOPm47cyw; AAMC_mlb_0=REGION|9; aam_uuid=76899979787170794542236012221046728072; s_ppvl=Chicago%2520Cubs%253A%2520Team%253A%2520Player%2520Information%2C26%2C26%2C1063%2C1440%2C788%2C1440%2C900%2C2%2CP; QSI_HistorySession=https%3A%2F%2Fwww.mlb.com%2Fplayer%2Fjeff-mcneil-643446~1579648492612|https%3A%2F%2Fwww.mlb.com%2Fplayer%2Fyu-darvish-506433~1579648542566; s_ppv=Chicago%2520Cubs%253A%2520Team%253A%2520Player%2520Information%2C34%2C19%2C1916%2C1440%2C788%2C1440%2C900%2C2%2CP; s_getNewRepeat=1579660603422-Repeat; s_lv=1579660603424; s_pvs=0; s_tps=12081; __cfduid=dcbbe7432a7a65d9b0ac3c578d4a3f3da1580541225; AMCV_A65F776A5245B01B0A490D44%40AdobeOrg=1099438348|MCIDTS|18303|MCMID|76932247013101732222268800691362456112|MCAAMLH-1581967536|9|MCAAMB-1581967536|RKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y|MCOPTOUT-1581369936s|NONE|MCAID|NONE|vVersion|2.1.0; _bts=b16c6bef-4286-4894-8545-283acf210216; s_sq=%5B%5BB%5D%5D'
        }
        print(url)
        if headers:
            s = requests.get(url, timeout=None, headers=headers).content
        else:
            s = requests.get(url, timeout=None).content

        print(s)
        if encoding:
            data = pd.read_csv(io.StringIO(s.decode(encoding)))
        else:
            data = pd.read_csv(io.StringIO(s))
        return data

    def large_request(self, start_date, end_date, step, parameters, url, verbose):
        """
        Break start and end date into smaller increments, collecting all data in small chunks and appending all
        results to a common fangraph_stats end_dt is the date strings for the final day of the query d1 and end_date are
        datetime objects for first and last day of query, for doing date math a third datetime object (d) will be
        used to increment over time for the several intermediate queries

        :param url:
        :param parameters:
        :param start_date:
        :param end_date:
        :param step:
        :param verbose:
        :return:
        """

        error_counter = 0  # count failed requests. If > X, break
        logger.info("This is a large query, it may take a moment to complete")
        dataframe_list = []
        intermediate_dt = start_date + timedelta(days=step)
        while intermediate_dt <= end_date:
            if (intermediate_dt.month < 4 and intermediate_dt.day < 15) or (
                    start_date.month > 10 and start_date.day > 14):
                if end_date.year > intermediate_dt.year:
                    logger.info('Skipping Off Season Dates')
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
                            logger.warning(
                                "Query unsuccessful for data from {} to {}. Skipping these dates.".format(
                                    start_date.strftime('%Y-%m-%d'),
                                    intermediate_dt.strftime('%Y-%m-%d')))
                        else:
                            dataframe_list.append(smaller_data_1)
                        error_counter = 0
                        break
            else:
                dataframe_list.append(data)
                logger.info(
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

    def get_statcast_data(self, start_dt=None, end_dt=None, team=None, detail=True, verbose=True):
        """
        Pulls Statcast play-level data from Baseball Savant for a given date range. If no arguments are provided, this
        will return yesterday's statcast data. If one date is provided, it will return that date's Statcast data.

        :param start_dt: the first date for which you want statcast data
        :param end_dt: the last date for which you want statcast data
        :param team: [optional] city abbreviation of the team you want data for (e.g. SEA or BOS)
        :param detail: Provide Play-Level Detail
        :param verbose:
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

    def single_game(self, game_pk, team=None):
        """
        Pulls statcast play-level data from Baseball Savant for a single game, identified by its MLB game ID (game_pk
        in statcast data)

        :param self:
        :param game_pk: 6-digit integer MLB game ID to retrieve
        :param team:
        :return: DataFrame
        """
        data = self.__single_game_request(game_pk)
        data = self.__postprocessing(data, team)
        return data

    @staticmethod
    def batter(start_dt, end_dt, player_id):
        """
        Pulls statcast batter-level data from Baseball Savant for a given batter.
        :param start_dt:
        :param end_dt: the final date for which you want data
        :param player_id:the player's MLBAM ID. Find this by calling plyball.playerid_lookup(last_name, first_name),
        finding the correct player, and selecting their key_mlbam.
        :return:
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
    def pitcher(start_dt, end_dt, player_id):
        """
        Pulls statcast pitch-level data from Baseball Savant for a given batter.
        batters_lookup[]=####&batters_lookup[]=######
        pitchers_lookup[]=####&pitchers_lookup[]=#####

        :param start_dt: the first date for which you want a player's sSatcast data
        :param end_dt: the final date for which you want data
        :param player_id: the player's MLBAM ID.
        :return:
        """

        start_dt, end_dt, player_id = sanitize_input(start_dt, end_dt, player_id)

        url = ('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium'
               '=&hfBBL=&hfNewZones=&hfGT=R|PO|S|=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent'
               '=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&pitchers_lookup[]={'
               '}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by'
               '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details& ')
        df = split_request(start_dt, end_dt, player_id, url)
        return df

    def milb_statcast(self, start_date, end_date, details, verbose):
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

        if details:
            parameters['is_aggregate'] = 'false'

        data = self.large_request(start_date,
                                  end_date,
                                  step=small_query_threshold,
                                  parameters=parameters,
                                  url=self.urls['milb'],
                                  verbose=verbose)

        return data

