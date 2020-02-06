import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta
import io
from pybaseballdata.utils import sanitize_input, split_request

"""
main.py
====================================
Module for all DataFrames originating from StatCast
"""

class StatCast(object):
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

    @staticmethod
    def small_request(start_dt, end_dt):
        """

        :param start_dt:
        :param end_dt:
        :return:
        """
        url = ("https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium"
               "=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent"
               "=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={"
               "}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name"
               "&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&").format(
            start_dt, end_dt)
        s = requests.get(url, timeout=None).content
        data = pd.read_csv(io.StringIO(
            s.decode('utf-8')))
        return data

    def large_request(self, end_dt, d1, d2, step, verbose):
        """
        break start and end date into smaller increments, collecting all data in small chunks and appending all
        results to a common dataframe end_dt is the date strings for the final day of the query d1 and d2 are
        datetime objects for first and last day of query, for doing date math a third datetime object (d) will be
        used to increment over time for the several intermediate queries

        :param end_dt:
        :param d1:
        :param d2:
        :param step:
        :param verbose:
        :return: DataFrame
        """
        error_counter = 0
        no_success_msg_flag = False
        print("This is a large query, it may take a moment to complete")
        dataframe_list = []
        d = d1 + timedelta(days=step)
        while d <= d2:
            if (d.month < 4 and d.day < 15) or (d1.month > 10 and d1.day > 14):
                if d2.year > d.year:
                    print('Skipping offseason dates')
                    d1 = d1.replace(month=3, day=15, year=d1.year + 1)
                    d = d1 + timedelta(days=step + 1)
                else:
                    break

            start_dt = d1.strftime('%Y-%m-%d')
            intermediate_end_dt = d.strftime('%Y-%m-%d')
            data = self.small_request(start_dt, intermediate_end_dt)
            if data.shape[0] > 1:
                dataframe_list.append(data)
            else:
                success = 0
                while success == 0:
                    data = self.small_request(start_dt, intermediate_end_dt)
                    if data.shape[0] > 1:
                        dataframe_list.append(data)
                        success = 1
                    else:
                        error_counter += 1
                    if error_counter > 2:
                        tmp_end = d - timedelta(days=1)
                        tmp_end = tmp_end.strftime('%Y-%m-%d')
                        smaller_data_1 = self.small_request(start_dt, tmp_end)
                        smaller_data_2 = self.small_request(intermediate_end_dt, intermediate_end_dt)
                        if smaller_data_1.shape[0] > 1:
                            dataframe_list.append(smaller_data_1)
                            print("Completed sub-query from {} to {}".format(start_dt, tmp_end))
                        else:
                            print("Query unsuccessful for data from {} to {}. Skipping these dates.".format(start_dt,
                                                                                                            tmp_end))
                        if smaller_data_2.shape[0] > 1:
                            dataframe_list.append(smaller_data_2)
                            print("Completed sub-query from {} to {}".format(intermediate_end_dt, intermediate_end_dt))
                        else:
                            print("Query unsuccessful for data from {} to {}. Skipping these dates.".format(
                                intermediate_end_dt, intermediate_end_dt))

                        no_success_msg_flag = True
                        error_counter = 0
                        break

            if verbose:
                if no_success_msg_flag is False:
                    print("Completed sub-query from {} to {}".format(start_dt, intermediate_end_dt))
                else:
                    no_success_msg_flag = False
            d1 = d + timedelta(days=1)
            d = d + timedelta(days=step + 1)

        if d1 > d2:
            pass
        else:
            start_dt = d1.strftime('%Y-%m-%d')
            data = self.small_request(start_dt, end_dt)
            dataframe_list.append(data)
            if verbose:
                print("Completed sub-query from {} to {}".format(start_dt, end_dt))

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

        not_numeric = ['sv_id', 'umpire', 'type', 'inning_topbot', 'bb_type', 'away_team', 'home_team', 'p_throws',
                       'stand', 'game_type', 'des', 'description', 'events', 'player_name', 'game_date', 'pitch_type',
                       'pitch_name']

        numeric_cols = ['release_speed', 'release_pos_x', 'release_pos_z', 'batter', 'pitcher', 'zone', 'hit_location',
                        'balls',
                        'strikes', 'game_year', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'on_3b', 'on_2b', 'on_1b',
                        'outs_when_up', 'inning',
                        'hc_x', 'hc_y', 'fielder_2', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'sz_top', 'sz_bot',
                        'hit_distance_sc', 'launch_speed', 'launch_angle', 'effective_speed', 'release_spin_rate',
                        'release_extension',
                        'game_pk', 'pitcher.1', 'fielder_2.1', 'fielder_3', 'fielder_4', 'fielder_5',
                        'fielder_6', 'fielder_7', 'fielder_8', 'fielder_9', 'release_pos_y',
                        'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 'woba_value', 'woba_denom',
                        'babip_value',
                        'iso_value', 'launch_speed_angle', 'at_bat_number', 'pitch_number', 'home_score', 'away_score',
                        'bat_score',
                        'fld_score', 'post_away_score', 'post_home_score', 'post_bat_score', 'post_fld_score']

        data[numeric_cols] = data[numeric_cols].astype(float)
        data['game_date'] = pd.to_datetime(data['game_date'], format='%Y-%m-%d')
        data = data.sort_values(['game_date', 'game_pk', 'at_bat_number', 'pitch_number'], ascending=False)
        valid_teams = ['MIN', 'PHI', 'BAL', 'NYY', 'LAD', 'OAK', 'SEA', 'TB', 'MIL', 'MIA',
                       'KC', 'TEX', 'CHC', 'ATL', 'COL', 'HOU', 'CIN', 'LAA', 'DET', 'TOR',
                       'PIT', 'NYM', 'CLE', 'CWS', 'STL', 'WSH', 'SF', 'SD', 'BOS', 'ARI', 'ANA', 'WAS']

        if team in valid_teams:
            data = data.loc[(data['home_team'] == team) | (data['away_team'] == team)]
        elif team is not None:
            raise ValueError('Error: invalid team abbreviation. Valid team names are: {}'.format(valid_teams))
        data = data.reset_index()
        return data

    @staticmethod
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

        start_dt, end_dt = self.sanitize_input(start_dt, end_dt)
        small_query_threshold = 5

        if start_dt and end_dt:
            date_format = "%Y-%m-%d"
            d1 = datetime.strptime(start_dt, date_format)
            d2 = datetime.strptime(end_dt, date_format)
            days_in_query = (d2 - d1).days
            if days_in_query <= small_query_threshold:
                data = self.small_request(start_dt, end_dt)
            else:
                data = self.large_request(start_dt, end_dt, d1, d2, step=small_query_threshold, verbose=verbose)

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
        :param player_id:the player's MLBAM ID. Find this by calling pybaseballdata.playerid_lookup(last_name, first_name),
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
                   '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details& ')
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
            'player_type': 'pitcher',
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
                   '=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent'
                   '=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&pitchers_lookup%5B%5D={'
                   '}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by'
                   '=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details& ')
            df = split_request(start_dt, end_dt, player_id, url)
            return df
