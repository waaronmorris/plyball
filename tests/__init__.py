from plyball.fangraph import FanGraphs
from plyball.ottoneu import Ottoneu
import pandas as pd

import unittest


class TestFanGraphs(unittest.TestCase):

    def test_pitching_table(self):
        fg = FanGraphs()
        pt = fg.get_pitching_table(2021)

        self.assertEqual(type(pt), pd.DataFrame)
        self.assertEqual(pt.shape, (885, 324))


class TestOttoneu(unittest.TestCase):

    def test_pitching_table(self):
        ot = Ottoneu(186)
        players = ot.players()

        self.assertEqual(type(players['batterResults']), pd.DataFrame)
        self.assertEqual(players['info'].shape[1], 21)
        self.assertEqual(players['stat'].shape[1], 18)


if __name__ == '__main__':
    unittest.main()
