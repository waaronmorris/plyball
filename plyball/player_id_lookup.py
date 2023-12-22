import logging

import pandas as pd
import requests
import structlog
from bs4 import BeautifulSoup

logger = structlog.getLogger(__name__)


def get_lookup_table() -> pd.DataFrame:
    """
    Generate a Dataframe of Players

    :return: DataFrame
    """

    logger.info('Gathering player lookup table. This may take a moment.')
    # Construct the URL to the CSV files in the repository
    repo_url = "https://github.com"
    csv_url = repo_url + "/chadwickbureau/register/tree/master/data"

    # Retrieve the HTML contents of the CSV URL
    logger.debug(f'Gathering {csv_url}')
    response = requests.get(csv_url)
    html = response.json()
    logger.debug(f'Gathered {len(html)} bytes')

    # Parse the HTML contents to find the URLs of the CSV files
    csv_links = []
    for item in html['payload']['tree']['items']:
        data_url = f"{csv_url}/{item['path']}"
        csv_links.append(data_url)
    logger.debug(f'Found {len(csv_links)} CSV files')

    # Loop through each CSV file and read it into a pandas DataFrame
    register_data = []
    for link in csv_links:
        logger.info(f'Gathering {link}')
        if 'people' in link:
            csv_url = 'https://raw.githubusercontent.com/chadwickbureau/register/master/data/' + link.split('/')[-1]
            logger.info(f'Gathering {csv_url}')
            df = pd.read_csv(csv_url, dtype={
                    'key_sr_nfl': object,
                    'key_sr_nba': object,
                    'key_sr_nhl': object
            })
            register_data.append(df)

    # Concatenate all the DataFrames into a single DataFrame
    table = pd.concat(register_data)

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
