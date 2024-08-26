import unittest

import pandas as pd

from plyball.ottoneu import Ottoneu


class TestOttoneu(unittest.TestCase):

    def test_pitching_table(self):
        ot = Ottoneu(186)
        players = ot.players()

        self.assertEqual(type(players['batterResults']), pd.DataFrame)
        self.assertEqual(players['info'].shape[1], 21)
        self.assertEqual(players['stat'].shape[1], 18)

    def test_league_transactions(self):
        ot = Ottoneu(186)
        transactions = ot.league_transactions()

        self.assertEqual(type(transactions), pd.DataFrame)