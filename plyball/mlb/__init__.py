import datetime as dt
import logging

import pandas as pd
import requests

from plyball.mlb.models import BoxScoreResponse

base_url = 'https://statsapi.mlb.com/api/v1/'

urls = {
    'schedule_by_day': base_url + "schedule/games/?sportId=1&date={run_date}",
    'line_ups': base_url + 'schedule?gamePk={game_pk}&language=en&hydrate=lineups,probablePitcher(note)',
    'game': base_url + "game/{game_pk}/boxscore?timecode={run_date}",
    'schedule': base_url + "schedule/?eventTypes=primary,secondary&sportId=1&startDate={start_date}&endDate={end_date}"
}

# TIMECODE_FORMAT = YYYYMMDD_HHMMSS
TIMECODE_FORMAT = "%Y%m%d_%H%M%S"

headers = {
        "Content-Type": "application/json"
}

logger = logging.getLogger(__name__)


class MLBStats(object):
    """
    Class for MLBStats

    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info('MLBStats Initialized')

    def get_game_lineups(self, game_pk) -> pd.DataFrame:
        """
        Get Lineups for Today's Games

        :return:
        """
        self.logger.info('Getting Lineups|{}'.format(urls['line_ups'].format(game_pk=game_pk)))
        lineups = []

        # Send a GET request to the API endpoint
        response = requests.get(urls['line_ups'].format(game_pk=game_pk), headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the JSON response
            data = response.json()

            # Check if the game data is available
            if "dates" in data and len(data["dates"]) > 0 and "games" in data["dates"][0] and len(
                    data["dates"][0]["games"]) > 0:
                game = data["dates"][0]["games"][0]

                # Get the projected lineups
                for team in ['home', 'away']:
                    try:
                        lineup = game["lineups"][f"{team}Players"]
                    except KeyError:
                        logger.info(f"No projected lineup for {team} team")
                        logger.info(game)
                        continue
                    for i, player_info in enumerate(lineup):
                        player = {
                                "date":          game["gameDate"],
                                "player_id":     player_info["id"],
                                "player_name":   player_info["fullName"],
                                "position":      player_info["primaryPosition"]["abbreviation"],
                                "batting_order": i + 1,
                                "team":          {
                                        "team_id":   data["dates"][0]["games"][0]["teams"][team]["team"]["id"],
                                        "team_name": data["dates"][0]["games"][0]["teams"][team]["team"]["name"],
                                }
                        }
                        lineups.append(player)
                return pd.DataFrame(lineups)
            else:

                raise Exception(f"Error retrieving the projected lineup for Game ID: {game_pk}")

        else:
            raise Exception(f"Error retrieving the projected lineup for Game ID: {game_pk}")

    @staticmethod
    def _process_game(game_data):
        """
        Process game data

        :param game_data:
        :return:
        """
        pass

    def get_lineups_by_day(self, run_date: dt.datetime) -> pd.DataFrame:
        """
        Get Schedule by Day

        :param run_date:
        :return:
        """
        self.logger.info('Getting Lineups|{}'.format(urls['schedule_by_day'].format(run_date=run_date)))
        games = []

        # Send a GET request to the API endpoint
        response = requests.get(urls['schedule_by_day'].format(run_date=run_date.strftime("%Y-%m-%d")), headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the JSON response
            data = response.json()

            # Check if the game data is available
            for date in data["dates"]:
                for game in date["games"]:
                    return self.get_game_lineups(game["gamePk"])

    def get_game_boxscore(self, game_pk: str, run_date: dt.datetime) -> pd.DataFrame:
        """
        Get Game Logs

        :param game_pk: MLB game ID
        :param run_date: Date of the game
        :return: DataFrame with player stats
        :raises: Exception if unable to retrieve or process game data
        """
        logger.info(
            'Getting Game Logs|{}'.format(
                urls['game'].format(game_pk=game_pk,
                                    run_date=run_date.strftime("%Y-%m-%d"))))

        # Send a GET request to the API endpoint
        try:
            response = requests.get(urls['game'].format(
                game_pk=game_pk,
                run_date=run_date.strftime(TIMECODE_FORMAT)),
                headers=headers)

            # Check if the request was successful
            if response.status_code == 200:
                # Get the JSON response
                try:
                    box_score = BoxScoreResponse(**response.json())
                    logger.info(f"Successfully parsed boxscore response for game {game_pk}")
                    player_stats = []

                    def process_player_stats(player, team_id):
                        try:
                            player_id = player.person.id
                            player_name = player.person.fullName
                            logger.debug(f"Processing player: {player_name} (ID: {player_id})")

                            stats_dict = {
                                'player_name': player_name,
                                'player_id': player_id,
                                'team': team_id,
                            }
                            
                            # Handle position data with logging
                            if player.position is not None:
                                stats_dict.update({
                                    'position_name': player.position.name,
                                    "position_type": player.position.type,
                                    "position_abbreviation": player.position.abbreviation,
                                })
                            else:
                                logger.warning(f"Player {player_name} (ID: {player_id}) has no position data")
                                stats_dict.update({
                                    'position_name': None,
                                    "position_type": None,
                                    "position_abbreviation": None,
                                })
                            
                            # Add batting order
                            stats_dict["batting_order"] = player.battingOrder
                            
                            # Handle game status with logging
                            if player.gameStatus is not None:
                                stats_dict.update({
                                    "batter_status": player.gameStatus.isCurrentBatter,
                                    "pitcher_status": player.gameStatus.isCurrentPitcher,
                                    "on_bench": player.gameStatus.isOnBench,
                                    "substitute": player.gameStatus.isSubstitute,
                                })
                            else:
                                logger.warning(f"Player {player_name} (ID: {player_id}) has no game status data")
                                stats_dict.update({
                                    "batter_status": False,
                                    "pitcher_status": False,
                                    "on_bench": False,
                                    "substitute": False,
                                })
                            
                            # Add jersey number
                            stats_dict["jersey_number"] = player.jerseyNumber

                            # Handle stats data with logging
                            if player.stats is not None:
                                if hasattr(player.stats, 'batting'):
                                    for key, value in player.stats.batting.items():
                                        stats_dict[f'batting_{key}'] = value
                                else:
                                    logger.debug(f"Player {player_name} (ID: {player_id}) has no batting stats")

                                if hasattr(player.stats, 'pitching'):
                                    for key, value in player.stats.pitching.items():
                                        stats_dict[f'pitching_{key}'] = value
                                else:
                                    logger.debug(f"Player {player_name} (ID: {player_id}) has no pitching stats")

                                if hasattr(player.stats, 'fielding'):
                                    for key, value in player.stats.fielding.items():
                                        stats_dict[f'fielding_{key}'] = value
                                else:
                                    logger.debug(f"Player {player_name} (ID: {player_id}) has no fielding stats")
                            else:
                                logger.warning(f"Player {player_name} (ID: {player_id}) has no stats data")

                            return stats_dict
                        except Exception as e:
                            logger.error(f"Error processing player data: {e}")
                            # Return basic info to avoid breaking the entire pipeline
                            return {
                                'player_name': player.person.fullName if hasattr(player, 'person') else 'Unknown',
                                'player_id': player.person.id if hasattr(player, 'person') else 0,
                                'team': team_id,
                                'error': str(e)
                            }

                    # Process home team players
                    logger.info(f"Processing home team players for game {game_pk}")
                    home_team_id = box_score.teams.home.team.id
                    for player_id, player in box_score.teams.home.players.items():
                        try:
                            player_stats.append(process_player_stats(player, home_team_id))
                        except Exception as e:
                            logger.error(f"Error processing home player {player_id}: {e}")

                    # Process away team players
                    logger.info(f"Processing away team players for game {game_pk}")
                    away_team_id = box_score.teams.away.team.id
                    for player_id, player in box_score.teams.away.players.items():
                        try:
                            player_stats.append(process_player_stats(player, away_team_id))
                        except Exception as e:
                            logger.error(f"Error processing away player {player_id}: {e}")

                    logger.info(f"Successfully processed {len(player_stats)} players for game {game_pk}")
                    return pd.DataFrame(player_stats)
                except Exception as e:
                    logger.error(f"Error parsing box score data for game {game_pk}: {str(e)}")
                    raise Exception(f"Error parsing box score data for game {game_pk}: {str(e)}")
            else:
                logger.error(f"API request failed with status code {response.status_code}: {response.text}")
                raise Exception(f"Error retrieving the game data for Game ID: {game_pk}. Status code: {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Request failed for game {game_pk}: {str(e)}")
            raise Exception(f"Network error retrieving game data for Game ID: {game_pk}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error processing game {game_pk}: {str(e)}")
            raise Exception(f"Unexpected error processing game data for Game ID: {game_pk}: {str(e)}")

    def get_schedule(self, start_date: dt.datetime, end_date: dt.datetime) -> pd.DataFrame:
        """
        Get Schedule

        :return:
        """
        self.logger.info('Getting Schedule|{}'.format(urls['schedule']))
        games = []

        # Send a GET request to the API endpoint
        response = requests.get(urls['schedule'].format(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            headers=headers))

        # Check if the request was successful
        if response.status_code == 200:
            # Get the JSON response
            data = response.json()

            # Check if the game data is available
            for date in data["dates"]:
                for game in date["games"]:
                    games.append(game)

            return pd.DataFrame(games)
        else:
            raise Exception(f"Error retrieving the schedule")
