import pandas as pd


def get_position_dummies(player_info):
    """

    :param player_info:
    :return:
    """
    position_info = player_info[['PlayerID','Positions']].copy()
    position_info['Positions'] = position_info.Positions.str.split('/')
    player_info = player_info.merge(pd.get_dummies(position_info.set_index('PlayerID').Positions. \
                                                   apply(pd.Series). \
                                                   reset_index(). \
                                                   melt(id_vars=['PlayerID'],
                                                        var_name='_ignore',
                                                        value_name='position')[['PlayerID','position']]. \
                                                   dropna()).groupby('PlayerID').sum(),
                                    on='PlayerID')
    return player_info.copy()


def stage_player_transactions(df):
    df = df.sort_values(['player_id', 'Date'], ascending=(True, True))
    shift_transactions = df.groupby('player_id').shift(-1)
    shift_transactions.columns = ['{}_{}'.format('next', column) for column in shift_transactions.columns]
    return pd.concat([df, shift_transactions], axis=1)


def get_player_scd(df):
    scd_columns = ['player_id',
                   'Transaction Type',
                   'team_id',
                   'Salary',
                   'Date',
                   'next_Date', ]

    df = df[scd_columns].copy()
    df.columns = ['player_id', 'transaction_type', 'team_id', 'salary', 'from_date', 'to_date']
    df['to_date'] = df['to_date'].fillna(pd.to_datetime('12/31/2050', format='%m/%d/%Y'))
    return df


def get_salary_fact(df):
    fact_columns = ['player_id',
                    'Transaction Type',
                    'next_Transaction Type',
                    'team_id',
                    'next_team_id',
                    'Salary',
                    'next_Salary',
                    'Date',
                    'next_Date', ]

    column_names = ['player_id', 'transaction_type', 'next_transaction_type',
                    'team_id', 'next_team_id', 'salary', 'next_salary', 'transaction_date', 'next_transaction_date']

    df = df[fact_columns].copy()
    df.columns = column_names
    df['salary_change'] = df['next_salary'] - df['salary']
    df['days_to_next_transaction'] = pd.to_datetime(df['next_transaction_date']) - pd.to_datetime(
        df['transaction_date'])
    return df