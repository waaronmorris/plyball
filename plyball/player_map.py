import pandas as pd


def get_player_map():
    sheet_id = '1JgczhD5VDQ1EiXqVG-blttZcVwbZd5_Ne_mefUGwJnk'
    sheet_name = 'PLAYERIDMAP'
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
    return pd.read_csv(url)

