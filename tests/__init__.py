import unittest

import pandas as pd

from plyball.fangraphs import FanGraphs



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

        print(1)

if __name__ == '__main__':
    unittest.main()
