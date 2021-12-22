import io
import logging

import pandas as pd
import requests


def get_lookup_table() -> pd.DataFrame:
    """
    Generate a Dataframe of Players

    :return: DataFrame
    """
    logging.info('Gathering player lookup table. This may take a moment.')
    url = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
    s = requests.get(url).content
    table = pd.read_csv(io.StringIO(s.decode('utf-8')), dtype={'key_sr_nfl': object,
                                                               'key_sr_nba': object,
                                                               'key_sr_nhl': object
                                                               })

    table['name_last'] = table['name_last'].str.lower()
    table['name_first'] = table['name_first'].str.lower()
    table[['key_mlbam', 'key_fangraphs']] = table[['key_mlbam', 'key_fangraphs']].fillna(-1)
    table[['key_mlbam', 'key_fangraphs']] = table[['key_mlbam', 'key_fangraphs']].astype(int)
    return table


def player_id_lookup(last_name: str, first_name: str = None) -> pd.DataFrame:
    """
    Obtain IDs for single Player

    :param last_name: Last Name of Player
    :type last_name: string
    :param first_name: First Name of Player
    :type first_name: string
    """
    last_name = last_name.lower()
    if first_name:
        first_name = first_name.lower()
    table = get_lookup_table()
    if first_name is None:
        results = table.loc[table['name_last'] == last_name]
    else:
        results = table.loc[(table['name_last'] == last_name) & (table['name_first'] == first_name)]

    results = results.reset_index().drop('index', 1)
    return results


def player_id_reverse_lookup(player_ids, key_type=None) -> pd.DataFrame:
    """
    Retrieve a table of player information given a list of player ids

    :param List player_ids: list of player ids
    :param string key_type: name of the key player_type being looked up (one of "mlbam", "retro", "bbref", or "fangraphs")
    :return: DataFrame
    """
    key_types = ('mlbam', 'retro', 'bbref', 'fangraphs',)

    if not key_type:
        key_type = key_types[0]  # default is "mlbam" if key_type not provided
    elif key_type not in key_types:
        raise ValueError(
            '[Key Type: {}] Invalid; Key Type must be one of "{}"'.format(key_type, '", "'.join(key_types))
        )

    table = get_lookup_table()
    key = 'key_{}'.format(key_type)

    results = table[table[key].isin(player_ids)]
    results = results.reset_index().drop('index', 1)
    return results
