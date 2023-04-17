import io
import logging
from datetime import datetime, timedelta, date
from typing import Literal

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from plyball.utils import first_season_map


class BaseballReference(object):
    """
    Class for BaseballReference
    """

    urls = {
        'daily_war': 'http://www.baseball-reference.com/data/war_daily_{}.txt',
        'player_type': 'http://www.baseball-reference.com/leagues/daily.cgi?{}',
        'standings': 'http://www.baseball-reference.com/leagues/MLB/{}-standings.shtml',
        'team_results': 'http://www.baseball-reference.com/teams/{}/{}-schedule-scores.shtml'
    }

    def __get_league_stats_html(self, position_type: Literal['p', 'b'],
                                start_date: date,
                                end_date: date):
        """
        Get League Stats from Baseball Reference for pitching and batting.

        :param DateTime start_date: Date to Begin Stats Lookup
        :param DateTime end_date: Date to End Stats Lookup
        :param string position_type: pitching (p) or batting (b)
        :return: BeautifulSoup
        """

        parameters = {
            'user_team': '',
            'bust_cache': '',
            'type': position_type,
            'lastndays': '7',
            'dates': 'fromandto',
            'fromandto': '{}.{}'.format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
            'level': 'mlb',
            'franch': '',
            'stat': '',
            'stat_value': '0',
        }

        logging.info(self.urls['player_type'].format('&'.join(['{}={}'.format(k, v)
                                                               for k, v in parameters.items()])))
        s = requests.get(self.urls['player_type'].format('&'.join(['{}={}'.format(k, v)
                                                                   for k, v in parameters.items()]))).content

        return BeautifulSoup(s, "lxml")

    def __get_league_stats_table(self,
                                 position_type: Literal['b', 'p'],
                                 start_date: date = datetime.today(),
                                 end_date: date = datetime.today() - timedelta(1)):
        """
        Get player_type from Baseball Reference for a certain time frame.

        :param string position_type: Pitching (p) or Batting (b)
        :param DateTime start_date: Beginning Date
        :param DateTime end_date: Ending Date
        :return: DataFrame
        """
        soup = self.__get_league_stats_html(position_type, start_date, end_date)
        table = soup.find_all('table')[0]
        data = []
        headings = [th.get_text() for th in table.find("tr").find_all("th")][1:]
        data.append(headings)
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols])
        data = pd.DataFrame(data[1:], columns=data[0])
        return data.copy()

    def __get_team_result_html(self, season: int, team: str):
        s = requests.get(self.urls['team_results'].format(team, season)).content
        return BeautifulSoup(s, "lxml")

    @staticmethod
    def __process_win_streak(data):
        if data['Streak'].count() > 0:
            data['Streak2'] = data['Streak'].str.len()
            data.loc[data['Streak'].str[0] == '-', 'Streak2'] = -data['Streak2']
            data['Streak'] = data['Streak2']
            data = data.drop('Streak2', 1)
        return data

    def schedule_and_record(self, season: int, team: str):
        """
        Get a teams schedule and record from Baseball Reference

        :param season: Season Year
        :param team: Team Initials
        :return: DataFrame
        """
        try:
            if season < first_season_map[team]:
                m = "Season cannot be before first_name year of a team's existence"
                raise ValueError(m)
        # ignore validation if team isn't found in dictionary
        except KeyError:
            pass
        if season > datetime.now().year:
            raise ValueError('Season cannot be after current year')

        team_results = self.__get_team_results_table(season, team)
        team_results = self.__process_win_streak(team_results)
        for column in ["R", "RA", "Inn", "Rank", "Attendance"]:
            team_results[column] = pd.to_numeric(team_results[column], errors='coerce')
        return team_results

    def __get_team_results_table(self, season: int, team: str):
        try:
            table = self.__get_team_result_html(season, team).find_all('table')[0]
        except:
            raise ValueError("Data cannot be retrieved for this team/year combo. Please verify that your team "
                             "abbreviation is accurate and that the team existed during the season you are searching "
                             "for.")
        data = []
        headings = [th.get_text() for th in table.find("tr").find_all("th")]
        headings = headings[1:]  # the "gm#" heading doesn't have a <td> element
        headings[3] = "Home_Away"
        data.append(headings)
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row_index in range(len(rows) - 1):  # last_name row is a description of column meanings
            row = rows[row_index]
            try:
                cols = row.find_all('td')
                # links = row.find_all('a')
                if cols[1].text == "":
                    cols[
                        1].string = team
                if cols[3].text == "":
                    cols[3].string = 'Home'
                if cols[12].text == "":
                    cols[12].string = "None"
                if cols[13].text == "":
                    cols[13].string = "None"
                if cols[14].text == "":
                    cols[14].string = "None"
                if cols[8].text == "":
                    cols[8].string = "9"
                if cols[16].text == "":
                    cols[16].string = "Unknown"
                if cols[15].text == "":
                    cols[15].string = "Unknown"
                if cols[17].text == "":
                    cols[17].string = "Unknown"

                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])
            except:
                # two cases will break the above: games that haven't happened yet, and BR's redundant mid-table headers
                # if future games, grab the scheduling info. Otherwise do nothing.
                if len(cols) > 1:
                    cols = [ele.text.strip() for ele in cols][0:5]
                    data.append([ele for ele in cols if ele])
        data = pd.DataFrame(data, columns=data.iloc[0])
        data['Attendance'].replace(r'^Unknown$', np.nan, regex=True, inplace=True)
        return data

    def get_stats_range(self, position_type: Literal['p', 'b'], start_dt: date = None, end_dt: date = None):
        """
        Get player_type for a set time range. This can be the past week, the
        month of August, anything. Just supply the start and end date in YYYY-MM-DD
        format.

        :param string position_type: Pitching (p) or Batting (b)
        :param DateTime start_dt:
        :param DateTime end_dt:
        :return: DataFrame
        """
        """

        """
        table = self.__get_league_stats_table(position_type, start_dt, end_dt)

        if position_type == 'p':
            table = table.replace('---%', np.NaN)
            for column in ['Age', '#days', 'G', 'GS', 'W', 'L', 'SV', 'IP', 'H',
                           'R', 'ER', 'BB', 'SO', 'HR', 'HBP', 'ERA', 'AB', '2B',
                           '3B', 'IBB', 'GDP', 'SF', 'SB', 'CS', 'PO', 'BF', 'Pit',
                           'WHIP', 'BAbip', 'SO9', 'SO/W']:
                table[column] = pd.to_numeric(table[column])
            # convert str(xx%) values to float(0.XX) decimal values
            for column in ['Str', 'StL', 'StS', 'GB/FB', 'LD', 'PU']:
                table[column] = pd.to_numeric(table[column].replace('%', '', regex=True), errors='coerce') / 100
        elif position_type == 'b':
            for column in ['Age', '#days', 'G', 'PA', 'AB', 'R', 'H', '2B', '3B',
                           'HR', 'RBI', 'BB', 'IBB', 'SO', 'HBP', 'SH', 'SF', 'GDP',
                           'SB', 'CS', 'BA', 'OBP', 'SLG', 'OPS']:
                table[column] = pd.to_numeric(table[column], errors='coerce')
        table = table.drop('', 1)
        return table

    def get_season_stats(self, position_type: Literal['p', 'b'], season: int = datetime.now().year):
        """
        Get Stats from a set season.
        :param position_type: Pitching (p) or Batting (b)
        :param season: Season Year
        :return: DataFrame
        """

        start_dt = datetime(season, 3, 1)
        end_dt = datetime(season, 11, 1)
        return self.get_stats_range(position_type, start_dt, end_dt)

    def get_daily_war(self, position_type: Literal['p', 'b']):
        """
        Get data from Daily War tables (pitching or batting). Returns WAR, its components, and a few other useful player_type.

        :param string position_type: pitch or bat
        :return: DataFrame
        """
        s = requests.get(self.urls['daily_war'].format(position_type)).content
        c = pd.read_csv(io.StringIO(s.decode('utf-8')))
        return c
