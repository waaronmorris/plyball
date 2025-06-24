import datetime as dt
from io import StringIO
from typing import Dict, List, Union
import re

import numpy as np
import pandas as pd
import requests
import structlog
from bs4 import BeautifulSoup
from pandas import DataFrame

logger = structlog.get_logger(__name__)

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
        self.logger.debug(data[:-1])

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
        self.logger.debug(data[:-1])

        a = requests.post("{}/ajax/search".format(self.ottoneu_base_url),
                          data=data,
                          headers=headers)

        _json = a.json()
        self.logger.debug(f'JSON: {_json}')
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

    def league_transactions(self,
                            to_date: dt.datetime = None,
                            from_date: dt.datetime = None
                            ) -> pd.DataFrame:
        """
        Get Transaction Log of Transaction of Players in Fantasy League.

        :return: DataFrame
        """
        __start_time = dt.datetime.now()
        next_page = True
        df = pd.DataFrame()
        players = []
        teams = []
        trades = []
        adds = []

        __url = '{}/{}'.format(self.ottoneu_base_url, 'transactions')

        while __url:
            transaction_page = requests.get(__url)

            soup = BeautifulSoup(transaction_page.text, 'html.parser')

            try:
                table = soup.find_all('table')[0]
            except IndexError:
                logger.info('No Table Found on {}'.format(__url))
                table = None

            try:
                for row in table.find_all('tr'):
                    for link in row.find_all('a'):
                        url = link.get('href', None)
                        if 'player' in url:
                            # <a href="/186/players/31855">Alex Call</a>
                            players.append(int(url.split('/')[-1]))

                        elif 'team' in url:
                            try:
                                # <a href="/186/team/1419">Fake News Bears</a>
                                teams.append(int(url.split('/')[-1]))
                            except ValueError:
                                logger.info('No Team Found on {}'.format(__url))

                        elif 'viewtrade' in url:
                            # 'https://ottoneu.fangraphs.com/186/viewtrade?id=107928&pending=1'
                            parameters = url.split('?')[1].split('&')
                            for parameter in parameters:
                                p, v = parameter.split('=')
                                if p == 'id':
                                    trades.append(int(v))
                                    break

                        elif 'auctionresults' in url:
                            # https://ottoneu.fangraphs.com/186/auctionresults?id=1142035
                            parameters = url.split('?')[1].split('&')
                            for parameter in parameters:
                                p, v = parameter.split('=')
                                if p == 'id':
                                    adds.append(int(v))
                                    break


                    if len(trades) != len(players):
                        trades.append(None)

                    if len(adds) != len(players):
                        adds.append(None)

            except IndexError:
                logger.info('Players or Teams not found on {}'.format(__url))


            # df = df.(pd.read_html(str(table))[0])
            table_html = str(table)
            df = pd.concat([
                df,
                pd.read_html(StringIO(table_html))[0]],
                sort=False
            )

            earliest_date = pd.to_datetime(df['Date'],
                                           format='%b %d %Y %I:%M:%S %p').min()

            if to_date and earliest_date < to_date:
                break

            self.logger.info(next_page)
            next_page = soup.find_all('a', text="Next 50 transactions")
            if next_page:
                __url = '{}/{}?{}'.format(self.ottoneu_base_url,
                                          'transactions',
                                          ''.join(next_page[0].get('href').split('?')[1:]))
            else:
                __url = None


        df['team_id'] = np.asarray(teams)
        df['player_id'] = np.asarray(players)

        # Aug 25 2024 4:21:36 PM
        df['Date'] = pd.to_datetime(df['Date'], format='%b %d %Y %I:%M:%S %p')
        df['Salary'] = pd.to_numeric(df['Salary'].str.replace('$', ''))
        df['trade_id'] = np.asarray(trades)
        df['auction_id'] = np.asarray(adds)

        if to_date:
            df = df[df['Date'] <= to_date]

        if from_date:
            df = df[df['Date'] >= from_date]

        return df

    def get_line_up(self, team_id: int, date: str, stat_filter: str = 'today') -> pd.DataFrame:
        """
        Get Lineup of Players in Fantasy League.

        Args:
            team_id (int): Team ID in Ottoneu
            date (str): Date in YYYY-MM-DD format
            stat_filter (str): Stat filter (today, season, etc.)

        Returns:
            pd.DataFrame: DataFrame containing lineup information with columns for both batters and pitchers:
                Common columns:
                - player_id: Ottoneu player ID
                - name: Player name
                - position: Current position in lineup
                - player_positions: All eligible positions
                - is_pitcher: Whether player is a pitcher
                - is_position_player: Whether player is a position player
                - team: MLB team
                - salary: Player salary in Ottoneu
                - status: Player status (e.g., IL10, IL15)
                - game_status: Game status and score

                Batter specific columns:
                - batting_stats: AB, H, 2B, 3B, HR, BB, HBP, SB, CS
                - opponent_pitcher: Opposing pitcher name
                - opponent_pitcher_hand: Opposing pitcher handedness

                Pitcher specific columns:
                - pitching_stats: G, GS, IP, SV, HLD, K, BB, HBP, HR
                - pitch_count_last_5_days: Array of pitch counts for last 5 days
                - pitch_count_last_3_days: Array of pitch counts for last 3 days
        """
        url = f'{self.ottoneu_base_url}/setlineups?team={team_id}&date={date}&statFilter={stat_filter}'
        
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        players_data = []
        
        # Process both batter and pitcher tables
        for table in soup.find_all('table', {'class': 'lineup-table'}):
            try:
                # Determine if this is the pitcher table
                is_pitcher_table = 'pitcher' in table.get('class', [])
                
                # Get the stat headers for this table
                headers = []
                header_row = table.find('tr', {'class': 'statHeaders'})
                if header_row:
                    headers = [th.text.strip() for th in header_row.find_all('th')]
                
                # Process each player row
                for row in table.find_all('tr'):
                    try:
                        # Get the position cell which contains data attributes
                        pos_cell = row.find('td', {'class': 'lineup__button'})
                        if not pos_cell:
                            continue

                        # Extract all data attributes
                        player_data = {
                            'player_id': pos_cell.get('data-player-id'),
                            'position': pos_cell.get('data-position'),
                            'player_positions': pos_cell.get('data-player-positions'),
                            'is_pitcher': pos_cell.get('data-is-pitcher') == 'true',
                            'is_position_player': pos_cell.get('data-is-position-player') == 'true',
                            'is_pitcher_version_of_two_way_player': pos_cell.get('data-is-pitcher-version-of-two-way-player') == 'true'
                        }

                        # Get player name and details
                        name_cell = row.find('td', {'class': 'player-name'})
                        if name_cell:
                            # Get player name
                            name_link = name_cell.find('a')
                            if name_link:
                                player_data['name'] = name_link.text.strip()

                            # Get player bio info (team, position, handedness, salary)
                            bio_span = name_cell.find('span', {'class': 'lineup-player-bio'})
                            if bio_span:
                                bio_text = bio_span.find('span', {'class': 'strong tinytext'}).text.strip()
                                bio_parts = bio_text.split('&nbsp;')
                                if len(bio_parts) >= 3:
                                    player_data['team'] = bio_parts[0]
                                    player_data['listed_positions'] = bio_parts[1]
                                    player_data['handedness'] = bio_parts[2]

                                # Get salary
                                salary_span = bio_span.find('span', {'class': 'green tinytext strong'})
                                if salary_span:
                                    player_data['salary'] = int(salary_span.text.strip('$'))

                                # Get IL status if present
                                il_span = bio_span.find('span', {'class': 'tinytext morered'})
                                if il_span:
                                    player_data['status'] = il_span.text.strip()

                            # Get opponent info
                            opp_info = name_cell.find('span', {'class': 'lineup-opponent-info'})
                            if opp_info:
                                game_info = opp_info.find('span', {'class': 'lineup-game-info'})
                                if game_info:
                                    # Get game status and score
                                    game_text = game_info.text.strip()
                                    player_data['game_status'] = game_text

                                    if not is_pitcher_table:
                                        # Get opponent pitcher info for batters
                                        pitcher_link = opp_info.find('a', href=True)
                                        if pitcher_link:
                                            player_data['opponent_pitcher'] = pitcher_link.text.strip()
                                            # Get pitcher handedness
                                            hand_span = opp_info.find('span', {'class': 'tinytext'})
                                            if hand_span:
                                                player_data['opponent_pitcher_hand'] = hand_span.text.strip()

                        # Get pitch count data for pitchers
                        if is_pitcher_table:
                            pitch_count_container = row.find('td', {'class': 'pitch_count_container'})
                            if pitch_count_container:
                                # Last 5 days
                                five_day_table = pitch_count_container.find('table', {'class': 'pitch_count_last_five_days'})
                                if five_day_table:
                                    counts = [td.text.strip() for td in five_day_table.find_all('td')]
                                    player_data['pitch_count_last_5_days'] = counts

                                # Last 3 days
                                three_day_table = pitch_count_container.find('table', {'class': 'pitch_count_last_three_days'})
                                if three_day_table:
                                    counts = [td.text.strip() for td in three_day_table.find_all('td')]
                                    player_data['pitch_count_last_3_days'] = counts

                        # Get stats based on table type
                        stats_cells = row.find_all('td', {'class': 'stats-for-desktop'})
                        if stats_cells and headers:
                            stats_dict = {}
                            for i, cell in enumerate(stats_cells):
                                if i + 1 < len(headers):  # Add +1 to account for the Position column
                                    header = headers[i + 1].lower()
                                    stat_value = cell.text.strip()
                                    if stat_value and stat_value != 'Not Available':
                                        try:
                                            stat_value = float(stat_value)
                                        except ValueError:
                                            pass
                                        stats_dict[header] = stat_value
                            
                            # Store stats in appropriate field based on table type
                            if is_pitcher_table:
                                player_data['pitching_stats'] = stats_dict
                            else:
                                player_data['batting_stats'] = stats_dict

                        players_data.append(player_data)

                    except Exception as e:
                        logger.error(f'Error processing player row: {str(e)}')
                        continue

            except Exception as e:
                logger.error(f'Error processing table: {str(e)}')
                continue

        df = pd.DataFrame(players_data)
        
        # Clean up the DataFrame
        if not df.empty:
            # Convert player_id to numeric
            df['player_id'] = pd.to_numeric(df['player_id'], errors='coerce')
            
            # Convert salary to numeric
            if 'salary' in df.columns:
                df['salary'] = pd.to_numeric(df['salary'], errors='coerce')

        logger.info(f'Successfully retrieved lineup data for {len(df)} players')
        return df