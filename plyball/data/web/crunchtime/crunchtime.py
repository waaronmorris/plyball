import io
import requests
import pandas as pd


class CrunchTime(object):
    urls = {
        'master': 'http://crunchtimebaseball.com/master.csv'
    }

    def get_player_map(self):
        file = requests.get(self.urls['master']).content
        return pd.read_csv(io.StringIO(file.decode('Windows-1252')))
