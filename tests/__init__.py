import unittest

import pandas as pd

from plyball.fangraphs import FanGraphs
from plyball.ottoneu import Ottoneu


class TestOttoneu(unittest.TestCase):

    def test_pitching_table(self):
        ot = Ottoneu(186)
        players = ot.players()

        self.assertEqual(type(players['batterResults']), pd.DataFrame)
        self.assertEqual(players['info'].shape[1], 21)
        self.assertEqual(players['stat'].shape[1], 18)


class TestFanGraphs(unittest.TestCase):

    def test_tables(self):
        fg = FanGraphs()
        pt = fg.get_pitching_table(2021)

        fg = FanGraphs()
        pt = fg.get_batting_table(2021)

        self.assertEqual(type(pt), pd.DataFrame)
        self.assertEqual(pt.shape, (885, 324))

        fg = FanGraphs()
        pt = fg.get_milb_batting_table(2021)

        self.assertEqual(type(pt), pd.DataFrame)
        self.assertEqual(pt.shape, (885, 324))

        pt = fg.get_milb_pitching_table(2021)
        self.assertEqual(type(pt), pd.DataFrame)

        pt = fg.get_zip_bat_projections()
        self.assertEqual(type(pt), pd.DataFrame)

        pt = fg.get_zip_pitching_projections()
        self.assertEqual(type(pt), pd.DataFrame)

if __name__ == '__main__':
    unittest.main()
