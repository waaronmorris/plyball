from plyball.fangraph import FanGraphs
import pandas as pd

fg = FanGraphs()
pt = fg.get_pitching_table(2021)

assert type(pt) == pd.DataFrame
assert pt.shape == (885, 324)
