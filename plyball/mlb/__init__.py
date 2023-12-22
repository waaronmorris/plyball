import logging

import requests

base_url = 'https://statsapi.mlb.com/api/v1/'

urls = {
        'schedule_by_day': base_url + "schedule/games/?sportId=1&date={run_date}",
        'line_ups':        base_url + 'schedule?gamePk={game_pk}&language=en&hydrate=lineups,probablePitcher(note)'
}

headers = {
        "Content-Type": "application/json"
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MLBStats(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info('MLBStats Initialized')

    def get_game_lineups(self, game_pk):
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
                        print(game)
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
                return lineups
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

    def get_lineups_by_day(self, run_date):
        """
        Get Schedule by Day

        :param run_date:
        :return:
        """
        self.logger.info('Getting Lineups|{}'.format(urls['schedule_by_day'].format(run_date=run_date)))
        games = []

        # Send a GET request to the API endpoint
        response = requests.get(urls['schedule_by_day'].format(run_date=run_date), headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the JSON response
            data = response.json()

            # Check if the game data is available
            for date in data["dates"]:
                for game in date["games"]:
                    return self.get_game_lineups(game["gamePk"])
