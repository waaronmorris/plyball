import io
import requests
import pandas as pd
import chardet


class CrunchTime(object):
    urls = {
        'master': 'http://crunchtimebaseball.com/master.csv'
    }

    def get_player_map(self):
        file = requests.get(self.urls['master']).content
        print(chardet.detect(file))
        return pd.read_csv(io.StringIO(file.decode('latin-1')))
