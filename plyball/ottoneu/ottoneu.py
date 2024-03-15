from typing import Dict, List, Union

import numpy as np
import pandas as pd
import requests
import structlog
from bs4 import BeautifulSoup
from pandas import DataFrame


class Ottoneu(object):
    """
    A class for scrapping data from an Ottoneu pipeline league.

    """
    logger = structlog.get_logger("Ottoneu")

    def __init__(self, league_id: int):
        """
        Initialize Ottoneu with the ID of the League you are a part of.

        :param league_id:
        :type league_id: int
        """
        self.league_id = league_id
        self.ottoneu_base_url = 'https://ottoneu.fangraphs.com/{}'.format(league_id)

    @staticmethod
    def _process_player_page(soup: BeautifulSoup, stat_type: str, league: str = 'MLB') -> DataFrame:
        """
        Process a Player's page to extract player_type using Beautiful Soup

        :param soup: BS4 Website
        :param stat_type: 'Pitching' or 'Batting'
        :param league: 'MLB' or 'MILB'
        :type league: str
        :return: DataFrame
        """
        stats = pd.DataFrame()
        league_stats = soup.find_all("h3", text="{} Stats".format(league))
        if league_stats:
            if soup.find_all("h3", text="{} Stats".format(league))[0].parent.parent.find_all("h3", text=stat_type):
                stats = pd.read_html(str(soup.find_all("h3", text="{} Stats".format(league))[0].
                                         parent.parent.find_all("h3", text=stat_type)[0].
                                         parent.parent.find_all('table')))[0]
                stats['position_type'] = stat_type
                stats['league'] = "{} Stats".format(league)
        return stats

    def players(self, positions: List[str] = None) -> Dict[str, Union[DataFrame,
                                                                      Dict[str, DataFrame],
                                                                      Dict[str, DataFrame]]]:
        """
        Get DataFrame of players for Players listed.

        :param positions: ['All', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', 'UTIL']
        :type positions: List[str]
        :return: Dictionary consisting of various DataFrames of Players (and their Stats).
        """

        if positions is None:
            positions = ['C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP']

        headers = {
            'authority': 'ottoneu.fangraphs.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            'accept': '*/*',
            'origin': 'https://ottoneu.fangraphs.com',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'content-player_type': 'application/x-www-form-urlencoded',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': '{}/search'.format(self.ottoneu_base_url),
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9'
        }

        json_data = {
            'data': {
                'txtSearch': '',
                'selPos': positions,
                'playerLevel': 'majors',
                'chkFAOnly': '',
                'searchFilter': '',
                'searchComparison': '',
                'searchQualification': '',
            }
        }

        data = ''

        for wrapper, parameters in json_data.items():
            for param, value in parameters.items():
                if type(value) == list:
                    for selection in value:
                        data = data + '{}%5B{}%5D%5B%5D={}'.format(wrapper, param, selection)
                        data = data + '&'
                else:
                    data = data + '{}%5B{}%5D={}'.format(wrapper, param, value)
                data = data + '&'

        self.logger.info("{}/ajax/search".format(self.ottoneu_base_url))
        self.logger.info(data[:-1])

        a = requests.post("{}/ajax/search".format(self.ottoneu_base_url),
                          data=data,
                          headers=headers)

        _json = a.json()
        self.logger.info(f'JSON: {_json}')
        batter_info = []
        batter_stats = []
        for batter in _json['batterResults']:
            batter_info.append({k: v for k, v in batter.items() if k != 'Stats'})
            try:
                __stat_dict = {k: v for k, v in batter['Stats']['batting'].items()}
            except KeyError as e:
                self.logger.info(f"Batter: {batter['PlayerID']}|{e}")
                __stat_dict = {}
            except TypeError as e:
                self.logger.info(f"Batter: {batter['PlayerID']}|{e}")
                __stat_dict = {}
            __stat_dict['PlayerID'] = batter['PlayerID']
            batter_stats.append(__stat_dict)

        pitcher_info = []
        pitcher_stats = []
        for pitcher in _json['pitcherResults']:
            pitcher_info.append({k: v for k, v in pitcher.items() if k != 'Stats'})
            try:
                __stat_dict = {k: v for k, v in pitcher['Stats']['pitching'].items()}
            except KeyError as e:
                self.logger.info(f"Pitcher: {pitcher['PlayerID']}|{e}")
                __stat_dict = {}
            except TypeError as e:
                self.logger.info(f"Pitcher: {pitcher['PlayerID']}|{e}")
                __stat_dict = {}

            __stat_dict['PlayerID'] = pitcher['PlayerID']
            pitcher_stats.append(__stat_dict)

        df_batter_info = pd.DataFrame(batter_info)
        df_pitcher_info = pd.DataFrame(pitcher_info)

        df_batter_stat = pd.DataFrame(batter_stats)
        df_pitcher_stat = pd.DataFrame(pitcher_stats)

        # concat batter and pitcher info and stats rows into one dataframe
        df_info = pd.concat([df_batter_info, df_pitcher_info], sort=False)
        df_stat = pd.concat([df_batter_stat, df_pitcher_stat], sort=False)
        # df_info = df_batter_info.concat([df_pitcher_info], sort=False)
        # df_stat = df_batter_stat.append(df_pitcher_stat, sort=False)

        return {
            'info': df_info,
            'stat': df_stat,
            'batter': {
                'info': df_batter_info,
                'stat': df_batter_stat
            },
            'pitcher': {
                'info': df_pitcher_info,
                'stat': df_pitcher_stat
            }
        }

    def minor_players(self, positions: List[str] = None) -> Dict[str, Union[DataFrame,
    Dict[str, DataFrame],
    Dict[str, DataFrame]]]:
        """
        Get DataFrame of players for Players listed.

        :param positions: ['All', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', 'UTIL']
        :type positions: List[str]
        :return: Dictionary consisting of various DataFrames of Players (and their Stats).
        """

        if positions is None:
            positions = ['C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP']

        headers = {
            'authority': 'ottoneu.fangraphs.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            'accept': '*/*',
            'origin': 'https://ottoneu.fangraphs.com',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'content-player_type': 'application/x-www-form-urlencoded',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': '{}/search'.format(self.ottoneu_base_url),
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9'
        }

        json_data = {
            'data': {
                'txtSearch': '',
                'selPos': positions,
                'playerLevel': 'minors',
                'chkFAOnly': '',
                'searchFilter': '',
                'searchComparison': '',
                'searchQualification': '',
            }
        }

        data = ''

        for wrapper, parameters in json_data.items():
            for param, value in parameters.items():
                if type(value) == list:
                    for selection in value:
                        data = data + '{}%5B{}%5D%5B%5D={}'.format(wrapper, param, selection)
                        data = data + '&'
                else:
                    data = data + '{}%5B{}%5D={}'.format(wrapper, param, value)
                data = data + '&'

        self.logger.info("{}/ajax/search".format(self.ottoneu_base_url))
        self.logger.info(data[:-1])

        a = requests.post("{}/ajax/search".format(self.ottoneu_base_url),
                          data=data,
                          headers=headers)

        _json = a.json()
        self.logger.info(f'JSON: {_json}')
        batter_info = []
        batter_stats = []
        for batter in _json['batterResults']:
            batter_info.append({k: v for k, v in batter.items() if k != 'Stats'})
            try:
                __stat_dict = {k: v for k, v in batter['Stats']['batting'].items()}
            except KeyError as e:
                self.logger.info(f"Batter: {batter['PlayerID']}|{e}")
                __stat_dict = {}
            except TypeError as e:
                self.logger.info(f"Batter: {batter['PlayerID']}|{e}")
                __stat_dict = {}
            __stat_dict['PlayerID'] = batter['PlayerID']
            batter_stats.append(__stat_dict)

        pitcher_info = []
        pitcher_stats = []
        for pitcher in _json['pitcherResults']:
            pitcher_info.append({k: v for k, v in pitcher.items() if k != 'Stats'})
            try:
                __stat_dict = {k: v for k, v in pitcher['Stats']['pitching'].items()}
            except KeyError as e:
                self.logger.info(f"Pitcher: {pitcher['PlayerID']}|{e}")
                __stat_dict = {}
            except TypeError as e:
                self.logger.info(f"Pitcher: {pitcher['PlayerID']}|{e}")
                __stat_dict = {}

            __stat_dict['PlayerID'] = pitcher['PlayerID']
            pitcher_stats.append(__stat_dict)

        df_batter_info = pd.DataFrame(batter_info)
        df_pitcher_info = pd.DataFrame(pitcher_info)

        df_batter_stat = pd.DataFrame(batter_stats)
        df_pitcher_stat = pd.DataFrame(pitcher_stats)

        # concat batter and pitcher info and stats rows into one dataframe
        df_info = pd.concat([df_batter_info, df_pitcher_info], sort=False)
        df_stat = pd.concat([df_batter_stat, df_pitcher_stat], sort=False)
        # df_info = df_batter_info.concat([df_pitcher_info], sort=False)
        # df_stat = df_batter_stat.append(df_pitcher_stat, sort=False)

        return {
            'info': df_info,
            'stat': df_stat,
            'batter': {
                'info': df_batter_info,
                'stat': df_batter_stat
            },
            'pitcher': {
                'info': df_pitcher_info,
                'stat': df_pitcher_stat
            }
        }

    def player_details(self, player_id: str) -> pd.DataFrame:
        """
        Process an individual players statistics

        :param player_id: Ottoneu Player ID
        :return: DataFrame
        """
        headers = {
            'authority': 'ottoneu.fangraphs.com',
            'accept': '*/*',
            'origin': 'https://ottoneu.fangraphs.com',
            'x-requested-with': 'XMLHttpRequest',
            'content-player_type': 'application/x-www-form-urlencoded',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': '{}/search'.format(self.ottoneu_base_url),
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9'}

        player_page = requests.get('{}/playercard?id={}'.format(self.ottoneu_base_url, player_id), headers=headers)
        soup = BeautifulSoup(player_page.text, 'html.parser')

        # milb_pitching_stats = process_player_page(soup, 'Pitching', 'MILB')
        # milb_batting_stats = process_player_page(soup, 'Batting', 'MILB')

        player_detail_stats = (self._process_player_page(soup, 'Batting', 'MLB')
                               .append(self._process_player_page(soup, 'Pitching', 'MLB')))
        player_detail_stats['player_id'] = player_id

        return player_detail_stats

    def league_transactions(self) -> pd.DataFrame:
        """
        Get Transaction Log of Transaction of Players in Fantasy League.

        :return: DataFrame
        """
        next_page = True
        df = pd.DataFrame()
        players = []
        teams = []

        while next_page:
            transaction_page = requests.get('{}{}'.format(self.ottoneu_base_url, 'transactions'))
            soup = BeautifulSoup(transaction_page.text, 'html.parser')
            next_page = soup.find_all('a', text="Next 50 transactions")
            if next_page:
                next_page = next_page[0].get('href', None)
            else:
                next_page = None
            table = soup.find_all('table')[0]
            for link in table.find_all('a'):
                url = link.get('href', None)
                if 'playercard' in url:
                    players.append(int(url[url.find('=') + 1:]))
                elif 'team' in url:
                    teams.append(int(url[url.find('=') + 1:]))

            df = df.append(pd.read_html(str(table))[0])
            self.logger.info(next_page)

        df['team_id'] = np.asarray(teams)
        df['player_id'] = np.asarray(players)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Salary'] = pd.to_numeric(df['Salary'].str.replace('$', ''))
        return df

    def get_line_up(self) -> pd.DataFrame:
        """
        Get Lineup of Players in Fantasy League.

        :return:
        """
