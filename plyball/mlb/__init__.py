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

        :param game_pk:
        :return:
        """
        self.logger.info(
            'Getting Game Logs|{}'.format(
                urls['game'].format(game_pk=game_pk,
                                    run_date=run_date.strftime("%Y-%m-%d"))))

        # Send a GET request to the API endpoint
        response = requests.get(urls['game'].format(
            game_pk=game_pk,
            run_date=run_date.strftime(TIMECODE_FORMAT)),
            headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the JSON response
            box_score = BoxScoreResponse(**response.json())
            player_stats = []

            def process_player_stats(player, team_id):
                stats_dict = {
                    'player_name': player.person.fullName,
                    'player_id': player.person.id,
                    'team': team_id,
                    'position_name"': player.position.name,
                    "position_type": player.position.type,
                    "position_abbreviation": player.position.abbreviation,
                    "batting_order": player.battingOrder,
                    "batter_status": player.gameStatus.isCurrentBatter,
                    "pitcher_status": player.gameStatus.isCurrentPitcher,
                    "on_bench": player.gameStatus.isOnBench,
                    "substitute": player.gameStatus.isSubstitute,
                    "jersey_number": player.jerseyNumber,
                }

                for key, value in player.stats.batting.items():
                    stats_dict[key] = value

                for key, value in player.stats.pitching.items():
                    stats_dict[key] = value


                for key, value in player.stats.fielding.items():
                    stats_dict[key] = value

                return stats_dict

            for player_id, player in box_score.teams.home.players.items():
                player_stats.append(process_player_stats(player, box_score.teams.home.team.id))

            for player_id, player in box_score.teams.away.players.items():
                player_stats.append(process_player_stats(player, box_score.teams.away.team.id))

            return pd.DataFrame(player_stats)

        else:
            logger.info(response.text)
            raise Exception(f"Error retrieving the game data for Game ID: {game_pk}")

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
