import logging
import requests

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup


class Ottoneu(object):
    def __init__(self, league_id):
        self.league_id = league_id
        self.ottoneu_base_url = 'https://ottoneu.fangraphs.com/{}'.format(league_id)

    @staticmethod
    def __process_player_page(soup, stat_type, league='MLB'):
        """
        Process a Player's page to extract stats using Beautiful Soup

        :param soup: BS4 Website
        :param stat_type: 'Pitching' or 'Batting'
        :param league: 'MLB' or 'MILB'
        :return: DataFrame
        """
        if soup.find_all("h3", text="{} Stats".format(league))[0].parent.parent.find_all("h3", text=stat_type):
            stats = pd.read_html(str(soup.find_all("h3", text="{} Stats".format(league))[0].
                                     parent.parent.find_all("h3", text=stat_type)[0].
                                     parent.parent.find_all('table')))[0]
            stats['position_type'] = stat_type
            stats['league'] = "{} Stats".format(league)
        else:
            stats = pd.DataFrame()
        return stats

    def players(self, positions=None):
        """
        Get DataFrame of players for Players listed

        :param ottoneu_base_url: Ottoneu League URL [https://ottoneu.fangraphs.com/186]
        :param positions: ['All', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', 'UTIL']
        :return: dictionary consisting of various DataFrames of Players (and their Stats).
        """
        if positions is None:
            positions = ['', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', '']

        headers = {
            'authority': 'ottoneu.fangraphs.com',
            'accept': '*/*',
            'origin': 'https://ottoneu.fangraphs.com',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': '{}/search'.format(self.ottoneu_base_url),
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': 'PHPSESSID=l46n50mmf5iunhvmu29g9dp4uc; _ga=GA1.2.73209697.1579215532; _gid=GA1.2.124852174.1579215532; wordpress_test_cookie=WP+Cookie+check; __gads=ID=3142349aa5edeb87:T=1579215547:S=ALNI_MYMDjdj1jf3d6Qi1p3RADvVzpuxVg; wordpress_logged_in_0cae6f5cb929d209043cb97f8c2eee44=waaronmorris%7C1610772521%7C1wrLfEWarwbzufsFplT9gn55FlsSoJ3GbRd3e0wpC0a%7C991726ea6e2c3c687a01b737f1f3fc9e0ec789f18364f6240c229ee0989d9afc; amplitude_idfangraphs.com=eyJkZXZpY2VJZCI6IjMzMGEzNmRjLTE4ZjAtNDU0ZS1iOWZkLWYwNWNlZGY2MmY5MlIiLCJ1c2VySWQiOm51bGwsIm9wdE91dCI6ZmFsc2UsInNlc3Npb25JZCI6MTU3OTIyMDYxMDE4MywibGFzdEV2ZW50VGltZSI6MTU3OTIyMTA3MzgyOSwiZXZlbnRJZCI6MCwiaWRlbnRpZnlJZCI6MCwic2VxdWVuY2VOdW1iZXIiOjB9'
        }

        json_data = {'data': {'txtSearch': '',
                              'selPos': positions,
                              'playerLevel': 'all',
                              'chkFAOnly': '',
                              'searchFilter': '',
                              'searchComparison': '',
                              'searchQualification': '',
                              }
                     }

        data = ''
        for wrapper, paramters in json_data.items():
            for param, value in paramters.items():
                if type(value) == list:
                    for selection in value:
                        data = data + '{}[{}][]={}'.format(wrapper, param, selection)
                        data = data + '&'
                else:
                    data = data + '{}[{}]={}'.format(wrapper, param, value)
                data = data + '&'

        a = requests.post("{}/ajax/search".format(self.ottoneu_base_url),
                          data=data[:-1],
                          headers=headers)
        _json = a.json()

        batter_info = []
        batter_stats = []
        for batter in _json['batterResults']:
            batter_info.append({k: v for k, v in batter.items() if k != 'Stats'})
            if batter['Stats'] != 0:
                __stat_dict = {k: v for k, v in batter['Stats']['batting'].items()}
                __stat_dict['PlayerID'] = batter['PlayerID']
                batter_stats.append(__stat_dict)

        pitcher_info = []
        pitcher_stats = []
        for pitcher in _json['pitcherResults']:
            pitcher_info.append({k: v for k, v in pitcher.items() if k != 'Stats'})
            if pitcher['Stats'] != 0:
                if 'pitching' in pitcher['Stats']:
                    __stat_dict = {k: v for k, v in pitcher['Stats']['pitching'].items()}
                    __stat_dict['PlayerID'] = pitcher['PlayerID']
                    pitcher_stats.append(__stat_dict)

        df_batter_info = pd.DataFrame(batter_info)
        df_pitcher_info = pd.DataFrame(pitcher_info)

        df_batter_stat = pd.DataFrame(batter_stats)
        df_pitcher_stat = pd.DataFrame(pitcher_stats)

        df_info = df_batter_info.append(df_pitcher_info)
        df_stat = df_batter_stat.append(df_pitcher_stat)

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

    def player_details(self, player_id):
        """
        Process a individual players statistics

        :param player_id: Ottoneu Player ID
        :return: DataFrame
        """
        player_page = requests.get('{}/playercard?id={}'.format(self.ottoneu_base_url, player_id))
        soup = BeautifulSoup(player_page.text, 'html.parser')

        # milb_pitching_stats = process_player_page(soup, 'Pitching', 'MILB')
        # milb_batting_stats = process_player_page(soup, 'Batting', 'MILB')

        player_detail_stats = (Ottoneu.process_player_page(soup, 'Batting', 'MLB')
                               .append(Ottoneu.process_player_page(soup, 'Pitching', 'MLB')))
        player_detail_stats['player_id'] = player_id

        return player_detail_stats

    def league_transactions(self):
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
            logging.info(next_page)

        df['team_id'] = np.asarray(teams)
        df['player_id'] = np.asarray(players)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Salary'] = pd.to_numeric(df['Salary'].str.replace('$', ''))
        return df
