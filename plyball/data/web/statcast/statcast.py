import io
import logging
from datetime import timedelta
import requests
import numpy as np
import pandas as pd
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
        'search': 'https://baseballsavant.mlb.com/statcast_search/csv?{}'
    }

    @staticmethod
    def single_game_request(game_pk):
        """
        Select Single game for Statcast
        :param game_pk:
        :return:
        """
        url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&game_pk={game_pk}".format(
            game_pk=game_pk)
        s = requests.get(url, timeout=None).content
        data = pd.read_csv(io.StringIO(s.decode('utf-8')))
        return data

    def small_request(self, url):
        """

        :param start_dt:
        :param end_dt:
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
            'Referer': 'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea=2019%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=2019-08-01&game_date_lt=2019-08-31&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfPull=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_pas=0&chk_pitch_type=on&chk_pitch_result=on&chk_bb_type=on&chk_count=on&chk_batter_stands=on&chk_takes=on',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': 'AMCVS_A65F776A5245B01B0A490D44%40AdobeOrg=1; s_ecid=MCMID%7C76932247013101732222268800691362456112; _gcl_au=1.1.1513098462.1579648486; btIdentify=cfd6af57-74af-4b7d-a816-5ce347301cfb; _bti=%7B%22bsin%22%3A%22%22%7D; s_cc=true; _fbp=fb.1.1579648488603.1129893510; __gads=ID=cb7ecf3d7321b3a0:T=1579648489:S=ALNI_MZZwhO232BTMssupgFaZoOPm47cyw; AAMC_mlb_0=REGION%7C9; aam_uuid=76899979787170794542236012221046728072; s_ppvl=Chicago%2520Cubs%253A%2520Team%253A%2520Player%2520Information%2C26%2C26%2C1063%2C1440%2C788%2C1440%2C900%2C2%2CP; QSI_HistorySession=https%3A%2F%2Fwww.mlb.com%2Fplayer%2Fjeff-mcneil-643446~1579648492612%7Chttps%3A%2F%2Fwww.mlb.com%2Fplayer%2Fyu-darvish-506433~1579648542566; s_ppv=Chicago%2520Cubs%253A%2520Team%253A%2520Player%2520Information%2C34%2C19%2C1916%2C1440%2C788%2C1440%2C900%2C2%2CP; s_getNewRepeat=1579660603422-Repeat; s_lv=1579660603424; s_pvs=0; s_tps=12081; __cfduid=dcbbe7432a7a65d9b0ac3c578d4a3f3da1580541225; AMCV_A65F776A5245B01B0A490D44%40AdobeOrg=1099438348%7CMCIDTS%7C18303%7CMCMID%7C76932247013101732222268800691362456112%7CMCAAMLH-1581967536%7C9%7CMCAAMB-1581967536%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1581369936s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C2.1.0; _bts=b16c6bef-4286-4894-8545-283acf210216; s_sq=%5B%5BB%5D%5D'
        }

        s = requests.get(url, timeout=None, headers=headers).content
        data = pd.read_csv(io.StringIO(s.decode('utf-8')))
        return data

    def large_request(self, start_dt, end_dt, d1, d2, step, verbose):
        """
        break start and end date into smaller increments, collecting all data in small chunks and appending all
        results to a common fangraph_stats end_dt is the date strings for the final day of the query d1 and d2 are
        datetime objects for first and last day of query, for doing date math a third datetime object (d) will be
        used to increment over time for the several intermediate queries
        """

        parameters = {
            'all': 'true',
            'hfPT': '',
            'hfAB': '',
            'hfBBT': '',
            'hfPR': '',
            'hfZ': '',
            'stadium': '',
            'hfBBL': '',
            'hfNewZones': '',
            'hfGT': 'R%7C',
            'hfC': '',
            'hfSea': '2019%7C',
            'hfSit': '',
            'player_type': 'pitcher',
            'hfOuts': '',
            'opponent': '',
            'pitcher_throws': '',
            'batter_stands': '',
            'hfSA': '',
            'game_date_gt': '{}'.format(start_dt),
            'game_date_lt': '{}'.format(end_dt),
            'hfInfield': '',
            'team': '',
            'position': '',
            'hfOutfield': '',
            'hfRO': '',
            'home_road': '',
            'hfFlag': '',
            'hfPull': '',
            'metric_1': '',
            'hfInn': '',
            'min_pitches': '0',
            'min_results': '0',
            'group_by': 'name',
            'sort_col': 'pitches',
            'player_event_sort': 'h_launch_speed',
            'sort_order': 'desc',
            'min_pas': '0',
            'chk_pitch_type': 'on',
            'chk_pitch_result': 'on',
            'chk_bb_type': 'on',
            'chk_count': 'on',
            'chk_batter_stands': 'on',
            'chk_takes': 'on',
            'type': 'details&'
        }

        error_counter = 0  # count failed requests. If > X, break
        no_success_msg_flag = False  # a flag for passing over the success message of requests are failing
        logger.info("This is a large query, it may take a moment to complete")
        dataframe_list = []
        d = d1 + timedelta(days=step)
        while d <= d2:
            if (d.month < 4 and d.day < 15) or (d1.month > 10 and d1.day > 14):
                if d2.year > d.year:
                    logger.info('Skipping Off Season Dates')
                    d1 = d1.replace(month=3, day=15, year=d1.year + 1)
                    d = d1 + timedelta(days=step + 1)
                else:
                    break

            start_dt = d1.strftime('%Y-%m-%d')
            intermediate_end_dt = d.strftime('%Y-%m-%d')
            parameters['start_date'] = start_dt
            parameters['end_date'] = intermediate_end_dt
            data = self.small_request(self.urls['search'].format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
            if data.shape[0] > 1:
                dataframe_list.append(data)
            # if it failed, retry up to three times
            else:
                success = 0
                while success == 0:
                    parameters['start_date'] = start_dt
                    parameters['end_date'] = intermediate_end_dt
                    data = self.small_request(self.urls['search'].format(
                        '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
                    if data.shape[0] > 1:
                        dataframe_list.append(data)
                        success = 1
                    else:
                        error_counter += 1
                    if error_counter > 2:
                        tmp_end = d - timedelta(days=1)
                        tmp_end = tmp_end.strftime('%Y-%m-%d')
                        parameters['start_date'] = start_dt
                        parameters['end_date'] = tmp_end
                        smaller_data_1 = self.small_request(self.urls['search'].format(
                        '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))

                        parameters['start_date'] = intermediate_end_dt
                        parameters['end_date'] = intermediate_end_dt
                        smaller_data_2 = self.small_request(self.urls['search'].format(
                        '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
                        if smaller_data_1.shape[0] > 1:
                            dataframe_list.append(smaller_data_1)
                            logger.info("Completed sub-query from {} to {}".format(start_dt, tmp_end))
                        else:
                            logger.info("Query unsuccessful for data from {} to {}. Skipping these dates.".format(start_dt,
                                                                                                               tmp_end))
                        if smaller_data_2.shape[0] > 1:
                            dataframe_list.append(smaller_data_2)
                            logger.info(
                                "Completed sub-query from {} to {}".format(intermediate_end_dt, intermediate_end_dt))
                        else:
                            logger.info("Query unsuccessful for data from {} to {}. Skipping these dates.".format(
                                intermediate_end_dt, intermediate_end_dt))

                        no_success_msg_flag = True
                        error_counter = 0
                        break

            if verbose:
                if no_success_msg_flag is False:
                    logger.info("Completed sub-query from {} to {}".format(start_dt, intermediate_end_dt))
                else:
                    no_success_msg_flag = False  # if failed, reset this flag so message will send again next iteration
            # increment dates
            d1 = d + timedelta(days=1)
            d = d + timedelta(days=step + 1)

        # if start date > end date after being incremented, the loop captured each date's data
        if d1 > d2:
            pass
        # if start date <= end date, then there are a few leftover dates to grab data for.
        else:
            # start_dt from the earlier loop will work, but instead of d we now want the original end_dt
            start_dt = d1.strftime('%Y-%m-%d')
            data = self.small_request(self.urls['search'].format(
                '&'.join(['{}={}'.format(k, v) for k, v in parameters.items()])))
            dataframe_list.append(data)
            if verbose:
                print("Completed sub-query from {} to {}".format(start_dt, end_dt))

        # concatenate all dataframes into final result set
        final_data = pd.concat(dataframe_list, axis=0)
        return final_data

    @staticmethod
    def postprocessing(data, team):
        """

        :param data: Statcast Data to Process
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

    def statcast(self, start_dt=None, end_dt=None, team=None, verbose=True):
        """
        Pulls Statcast play-level data from Baseball Savant for a given date range. If no arguments are provided, this
        will return yesterday's statcast data. If one date is provided, it will return that date's Statcast data.

        :param self:
        :param start_dt: the first date for which you want statcast data
        :param end_dt: the last date for which you want statcast data
        :param team: [optional] city abbreviation of the team you want data for (e.g. SEA or BOS)
        :param verbose:
        :return: DataFrame
        """
        small_query_threshold = 5

        if start_dt and end_dt:
            days_in_query = (end_dt - start_dt).days
            if days_in_query <= small_query_threshold:
                data = self.small_request(start_dt, end_dt)
            else:
                data = self.large_request(start_dt.strftime('%Y-%m-%d'),
                                          end_dt.strftime('%Y-%m-%d'),
                                          start_dt,
                                          end_dt,
                                          step=small_query_threshold,
                                          verbose=verbose)

            data = self.postprocessing(data, team)
            return data

    @staticmethod
    def single_game(self, game_pk, team=None):
        """
        Pulls statcast play-level data from Baseball Savant for a single game, identified by its MLB game ID (game_pk
        in statcast data)

        :param self:
        :param game_pk: 6-digit integer MLB game ID to retrieve
        :param team:
        :return: DataFrame
        """
        data = self.single_game_request(game_pk)
        data = self.postprocessing(data, team)
        return data

    @staticmethod
    def batter(start_dt=None, end_dt=None, player_id=None):
        """
        Pulls statcast batter-level data from Baseball Savant for a given batter.
        :param start_dt:
        :param end_dt: the final date for which you want data
        :param player_id:the player's MLBAM ID. Find this by calling plyball.playerid_lookup(last_name, first_name),
        finding the correct player, and selecting their key_mlbam.
        :return:
        """
        parameters = {
            'all': 'true',
            'hfPT': 'FT',
            'hfAB': '',
            'hfBBT': '',
            'hfPR': '',
            'hfZ': '',
            'stadium': '',
            'hfBBL': '',
            'hfNewZones': '',
            'hfGT': 'R',
            'hfC': '',
            'hfSea': '2019',
            'hfSit': '',
            'player_type': 'batter',
            'hfOuts': '',
            'opponent': '',
            'pitcher_throws': '',
            'batter_stands': '',
            'hfSA': '',
            'game_date_gt': '',
            'game_date_lt': '',
            'hfInfield': '',
            'team': '',
            'position': '',
            'hfOutfield': '',
            'hfRO': '',
            'home_road': '',
            'hfFlag': '',
            'hfPull': '',
            'metric_1': '',
            'hfInn': '',
            'min_pitches': '0',
            'min_results': '0',
            'group_by': 'name',
            'sort_col': 'pitches',
            'player_event_sort': 'h_launch_speed',
            'sort_order': 'desc',
            'min_pas': 0}

        start_dt, end_dt, player_id = sanitize_input(start_dt, end_dt, player_id)
        if start_dt and end_dt:
            url = ('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium'
                   '=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=batter&hfOuts=&opponent'
                   '=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&batters_lookup%5B%5D={'
                   '}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by'
                   '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&player_type=details& ')
            df = split_request(start_dt, end_dt, player_id, url)
            return df

    @staticmethod
    def pitcher(start_dt=None, end_dt=None, player_id=None):
        """
        Pulls statcast pitch-level data from Baseball Savant for a given batter.

        :param start_dt: the first date for which you want a player's sSatcast data
        :param end_dt: the final date for which you want data
        :param player_id: the player's MLBAM ID.
        :return:
        """

        start_dt, end_dt, player_id = sanitize_input(start_dt, end_dt, player_id)

        if start_dt and end_dt:
            url = ('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium'
                   '=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent'
                   '=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&pitchers_lookup%5B%5D={'
                   '}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by'
                   '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&player_type=details& ')
            df = split_request(start_dt, end_dt, player_id, url)
            return df
