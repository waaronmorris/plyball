import requests
import pandas as pd
import datetime
import io
from bs4 import BeautifulSoup


class BaseballReference(object):
    urls = {
        'leaders': 'http://www.baseball-reference.com/leagues/daily.cgi'
    }

    def get_league_batting_stats(self, start_date, end_date):
        """

        :param start_date:
        :param end_date:
        :return:
        """
        parameters = {
            'user_team': '',
            'bust_cache': '',
            'type': 'b',
            'lastndays': '7',
            'dates': 'fromandto',
            'fromandto': '{}.{}'.format(start_date, end_date),
            'level': 'mlb',
            'franch': '',
            'stat': '',
            'stat_value': '0',
        }

        s = requests.get(url).content
        BeautifulSoup(s, "lxml")