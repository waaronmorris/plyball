import requests
import zipfile
import os
import pandas as pd
from io import BytesIO


class Lahman(object):
    """
    Pull data from [Sean Lahman's database](http://www.seanlahman.com/baseball-archive/statistics/), also hosted by
    Chadwick Bureau on GitHub -- our new source):
    """

    _handle = None
    _url = "https://github.com/chadwickbureau/baseballdatabank/archive/master.zip"
    _base_string = os.path.join("baseballdatabank-master", "core")

    def __init__(self):
        self.z = self.__get_lahman_zip()

    def __get_lahman_zip(self):
        """
        Retrieve the Lahman database zip file, returns None if file already exists in cwd.
        If we already have the zip file, keep re-using that.

        :return: zipFile
        """
        if os.path.exists(self._base_string):
            self._handle = None
        elif not self._handle:
            s = requests.get(self._url, stream=True)
            self._handle = zipfile.ZipFile(BytesIO(s.content))
        return self._handle

    def __download_lahman(self):
        """
        Download entire lahman db to present working directory

        :return: None
        """
        z = self.__get_lahman_zip()
        if z is not None:
            z.extractall(self._handle)
            z = self.__get_lahman_zip()

    @property
    def parks(self):
        """
        List of major league ballparks

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Parks.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def all_star_full(self):
        """
        Get All Star Appearances

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "AllstarFull.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def appearances(self):
        """
        Details on the positions a player appeared at

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Appearances.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def awards_managers(self):
        """
        Awards won by managers

        :return: DataFrame
        """
        self.z = self.get_lahman_self.zip()
        f = os.path.join(self._base_string, "AwardsManagers.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def awards_players(self):
        """
        awards won by players

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "AwardsPlayers.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def awards_share_managers(self):
        """
        award voting for manager awards

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "AwardsShareManagers.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def awards_share_players(self):
        """
        award voting for player awards

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "AwardsSharePlayers.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def batting(self):
        """
        batting statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Batting.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def batting_post(self):
        """
        post-season batting statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "BattingPost.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def college_playing(self):
        """
        list of players and the colleges they attended

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "CollegePlaying.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def fielding(self):
        """
        fielding statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Fielding.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def fielding_of(self):
        """
        Outfield position data

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "FieldingOF.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def fielding_of_split(self):
        """
        post-season fielding data

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "FieldingOFsplit.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def fielding_post(self):
        """
        post-season fielding data

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "FieldingPost.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def hall_of_fame(self):
        """
        Hall of Fame voting data

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "HallOfFame.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def home_games(self):
        """
        Number of homegames played by each team in each ballpark

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "HomeGames.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def managers(self):
        """
        Managerial statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Managers.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def managers_half(self):
        """
        Split season data for managers

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "ManagersHalf.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    # Alias for people -- the new name for master
    @property
    def master(self):
        """
        Player names, DOB, and biographical info

        :return: DataFrame
        """
        return self.people()

    @property
    def people(self):
        """
        Player names, DOB, and biographical info

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "People.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def pitching(self):
        """
        Pitching statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Pitching.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def pitching_post(self):
        """
        Post-season pitching statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "PitchingPost.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def salaries(self):
        """
        post-season pitching statistics

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Salaries.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def schools(self):
        """
        list of colleges that players attended

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Schools.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',',
                           quotechar='"')  # different here bc of doublequotes used in some school names
        return data

    @property
    def series_post(self):
        """
        post-season series information

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "SeriesPost.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def teams(self):
        """
        yearly player_type and standings

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "Teams.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def teams_franchises(self):
        """
        Team Franchise

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "TeamsFranchises.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data

    @property
    def teams_half(self):
        """
        franchise information

        :return: DataFrame
        """
        f = os.path.join(self._base_string, "TeamsHalf.csv")
        data = pd.read_csv(f if self.z is None else self.z.open(f), header=0, sep=',', quotechar="'")
        return data
