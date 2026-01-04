import pytz
import json
import requests
import warnings

# Ignore all warnings
warnings.filterwarnings("ignore")

import pandas as pd

from pprint import pprint
from datetime import datetime, timedelta, timezone
from pandas.tseries.offsets import BDay


PATH_REFRESH_TOKEN_FILE = '../../configs/questrade_refresh_token.txt'
PATH_DATA_TRADES = 'data/questrade_trade_data.csv'
URL_FX_FRANKFURTER = "https://api.frankfurter.app/latest"
URL_ACCESS_TOKEN = 'https://login.questrade.com/oauth2/token?grant_type=refresh_token&refresh_token='
URL_ACCOUNTS = 'v1/accounts'

LIST_ACC_TYPES = ['TFSA', 'FHSA', 'RRSP', 'Cash']

MAP_COL_TRADES = {
    'Transaction Date': 'transaction_date',
    'Settlement Date': 'settlement_date',
    'Action': 'action',
    'Symbol': 'symbol',
    'Description': 'description',
    'Currency': 'currency',
    'Quantity': 'quantity',
    'Price': 'price',
    'Gross Amount': 'gross_amount',
    'Net Amount': 'net_amount',
    'Activity Type': 'activity_type',
    'Account Type': 'account_type',
    'Account #': 'account_no'
}

MAP_COLS_ACTIVITIES = {
    'transactionDate': 'transaction_date',
    'settlementDate': 'settlement_date',
    'action': 'action',
    'symbol': 'symbol',
    'description': 'description',
    'currency': 'currency',
    'quantity': 'quantity',
    'price': 'price',
    'grossAmount': 'gross_amount',
    'netAmount': 'net_amount',
    'type': 'activity_type',
    'accountType': 'account_type',
    'accountNo': 'account_no',
}

MAP_REPLACE_SYMBOL = {
    'G036320': 'UCSH.U.TO',
    '.NVDA': 'NVDA.TO',
    'UCSH.U': 'UCSH.U.TO',
    '.META': 'META.TO',
    'VFV': 'VFV.TO',
    '.CNQ': 'CNQ.TO',
    'N460003': 'NKE',
    'C012145': 'CMCSA',
    'M413225': 'MU',
    '.CRM': 'CRM.TO',
    '.AQN': 'AQN.TO',
    'DGS': 'DGS.TO',
    'P007637': 'OXY',
    'A014251': 'MO',
    '.AVGO': 'AVGO.TO',
    '.UNH': 'UNH.TO',
    '': '',
}

LIST_DATE_COLS = ['transaction_date', 'settlement_date']
LIST_SORT_COLS = ['settlement_date', 'account_type', 'activity_type', 'symbol', 'net_amount']
LIST_SORT_ORDERS = [True, True, False, True, False]

local_tz = pytz.timezone("America/Toronto")
FORMAT_DATE = '%Y-%m-%d'


def init_server(token):
    global API_SERVER, headers
    # Step 1: Get access token and API server
    dict_token_data = get_access_token(refresh_token=token)
    API_SERVER = dict_token_data['api_server']
    access_token = dict_token_data['access_token']
    refresh_token = dict_token_data['refresh_token']
    
    headers = {'Authorization': f'Bearer {access_token}'}

    return refresh_token


def get_fx_rate(curr_from='USD', curr_to='CAD'):
    """Fetches the latest USD to CAD exchange rate using the Frankfurter API (no key needed)."""
    
    url = f"{URL_FX_FRANKFURTER}?from={curr_from}&to={curr_to}"
    
    try:
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()
        
        cad_rate = data['rates'].get('CAD')
        
        if cad_rate:
            return cad_rate
        else:
            print("Error: CAD rate not found in response.")
            return 1.40

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
        return 1.40


def get_access_token(refresh_token=None):
    '''Fetch a new access token using the refresh token.'''

    if not refresh_token:
        with open(PATH_REFRESH_TOKEN_FILE, 'r') as file:
            refresh_token = file.read()

    url = f'{URL_ACCESS_TOKEN}{refresh_token}'
    response = requests.get(url)
    response.raise_for_status()
    dict_token_data = response.json()

    access_token = dict_token_data['access_token']
    api_server = dict_token_data['api_server']  ##  https://api01.iq.questrade.com/
    refresh_token = dict_token_data['refresh_token']

    with open(PATH_REFRESH_TOKEN_FILE, 'w') as f:
        f.write(refresh_token)

    return dict_token_data  # Contains access_token, api_server, token_type, expires_in, refresh_token


def get_account_data():
    '''Fetch account information.'''

    url = f'{API_SERVER}{URL_ACCOUNTS}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response = response.json()

    return response


def get_balance(acc_no):
    '''Fetch account balance.'''
    url = f'{API_SERVER}{URL_ACCOUNTS}/{acc_no}/balances'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_positions(acc_no):
    '''Fetch account positions.'''
    url = f'{API_SERVER}{URL_ACCOUNTS}/{acc_no}/positions'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def search_symbol(symbol):
    '''Search for a symbol.'''
    url = f'{API_SERVER}v1/symbols/search?prefix={symbol}'
    print(url)
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_acc_nos(dict_acc_info, list_type_accs=LIST_ACC_TYPES):

    dict_acc_no = dict()

    for account in dict_acc_info['accounts']:
        if account['type'] in list_type_accs:
            dict_acc_no[account['type']] = account['number']

    return dict_acc_no
    

def get_acc_balances(dict_acc_info=None, list_type_accs=LIST_ACC_TYPES, list_acc_nos=None):

    dict_acc_balances = dict()

    if dict_acc_info and list_type_accs:
        dict_acc_nos = get_acc_nos(dict_acc_info, list_type_accs)
    
        for acc_type, acc_no in dict_acc_nos.items():
            balances = get_balance(acc_no)
            dict_acc_balances[acc_type] = balances
    elif list_acc_nos:
        for acc_no in list_acc_nos:
            balances = get_balance(acc_no)
            dict_acc_balances[acc_no] = balances

    return dict_acc_balances


def get_acc_positions(dict_acc_info=None, list_type_accs=LIST_ACC_TYPES, list_acc_nos=None):
    dict_acc_positions = dict()

    if dict_acc_info and list_type_accs:
        dict_acc_nos = get_acc_nos(dict_acc_info, list_type_accs)
    
        for acc_type, acc_no in dict_acc_nos.items():
            positions = get_positions(acc_no)
            dict_acc_positions[acc_type] = positions
    elif list_acc_nos:
        for acc_no in list_acc_nos:
            positions = get_positions(acc_no)
            dict_acc_positions[acc_no] = positions

    return dict_acc_positions


def get_activities(list_tickers, dict_acc_info=None, dict_acc_no=None, from_time=None, last_n_days=30):

    if not dict_acc_no:
        dict_acc_no = get_acc_nos(dict_acc_info)

    now = datetime.now(local_tz).replace(microsecond=0)

    if not from_time:
        from_time = now - timedelta(days=last_n_days)
    else:
        from_time = local_tz.localize(from_time)

    print(f'Fetching data from {from_time} to {now}')

    start_time = from_time.isoformat()
    end_time = now.isoformat()

    params = {
        "startTime": start_time,
        "endTime": end_time
    }

    dict_acc_act = dict()

    for acc_type, acc_no in dict_acc_no.items():
        url = f'{API_SERVER}{URL_ACCOUNTS}/{acc_no}/activities'

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        response = response.json()

        for act in response['activities']:
            act['accountNo'] = acc_no
            act['accountType'] = f'Individual {acc_type}'

        dict_acc_act[acc_type] = response

    return dict_acc_act


def format_activities(dict_acc_act):
    dict_act_tickers = dict()
    for acc_type, dict_act in dict_acc_act.items():
        list_act = dict_act['activities']
        dict_act_tickers[acc_type] = list()
        for act in list_act:
            dict_act_tickers[acc_type].append(f'{act["symbol"]}: {act["action"]}: {act["quantity"]} @ {act["price"]} = {act["netAmount"]} {act["currency"]} at {act["tradeDate"]}')

    return dict_act_tickers


def load_trades():
    df_trades = pd.read_csv(PATH_DATA_TRADES)
    df_trades = df_trades[list(MAP_COL_TRADES.keys())]
    df_trades.rename(columns=MAP_COL_TRADES, inplace=True)
    df_trades.replace(MAP_REPLACE_SYMBOL, inplace=True)
    df_trades.replace({'Individual cash': 'Individual Cash'}, inplace=True)
    df_trades.sort_values(
        by=LIST_SORT_COLS,
        ascending=LIST_SORT_ORDERS,
        inplace=True,
    )
    df_trades.reset_index(inplace=True, drop=True)
    df_trades.fillna('', inplace=True)
    
    for col in LIST_DATE_COLS:
        df_trades[col] = pd.to_datetime(df_trades[col], format='%Y-%m-%d %I:%M:%S %p').dt.strftime(FORMAT_DATE)

    latest_date = pd.to_datetime(df_trades['settlement_date'], format=FORMAT_DATE).max() - BDay(1)

    return df_trades, latest_date


def fetch_recent_activities(latest_date):
    dict_acc_info = get_account_data()
    dict_acc_no = get_acc_nos(dict_acc_info)
    # dict_acc_balances = get_acc_balances(dict_acc_info)
    dict_acc_act = get_activities(list_tickers=[], dict_acc_no=dict_acc_no, from_time=latest_date)

    list_df_act = list()
    
    for acc_type in LIST_ACC_TYPES:
        list_df_act.append(pd.DataFrame(dict_acc_act[acc_type]['activities']))
    
    df_activities = pd.concat(list_df_act)
    df_activities = df_activities[list(MAP_COLS_ACTIVITIES.keys())]
    df_activities.rename(columns=MAP_COLS_ACTIVITIES, inplace=True)
    df_activities['account_no'] = df_activities['account_no'].astype('int')
    df_activities['description'] = df_activities['description'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df_activities['action'] = df_activities['action'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df_activities.sort_values(
        by=LIST_SORT_COLS,
        ascending=LIST_SORT_ORDERS,
        inplace=True,
    )
    df_activities.reset_index(inplace=True, drop=True)
    df_activities.fillna('', inplace=True)
    
    for col in LIST_DATE_COLS:
        df_activities[col] = pd.to_datetime(df_activities[col], format='%Y-%m-%dT%H:%M:%S.%f%z')
        df_activities[col] = df_activities[col].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)
        df_activities[col] = df_activities[col].dt.strftime("%Y-%m-%d")

    return df_activities


def update_trades(df_trades, df_activities):
    key_cols = list(set(df_trades.columns) - {'transaction_date'})

    rows_to_add = df_activities.merge(df_trades[key_cols], on=key_cols, how="left", indicator=True)
    rows_to_add = rows_to_add[rows_to_add["_merge"] == "left_only"].drop(columns=["_merge"])

    df_trades_updated = pd.concat([df_trades, rows_to_add], ignore_index=True)
    return df_trades_updated


def get_trades(symbol, df_trades=None, account_type='', activity_type='', symbol_exact_match=False):
    if df_trades is None or df_trades.shape[0] == 0:
        df_trades, _ = load_trades()

    if symbol_exact_match:
        df_filtered = df_trades[df_trades['symbol'] == symbol]
    else:
        df_filtered = df_trades[df_trades['symbol'].str.contains(symbol, case=False, na=False)]

    if account_type:
        df_filtered = df_filtered[df_filtered['account_type'].str.contains(account_type, case=False, na=False)]

    if activity_type:
        df_filtered = df_filtered[df_filtered['activity_type'].str.contains(activity_type, case=False, na=False)]

    if df_filtered.shape[0] == 0:
        msg = f'No activities found for symbol: {symbol}'
        if account_type:
            msg = f'{msg} in account: {account_type}'

        return msg

    return df_filtered.reset_index(drop=True)


def save_updated_trades(df_trades_updated):
    cur_date = datetime.now().strftime("%Y%m%d")
    path_cur_date = f'{PATH_DATA_TRADES}_{cur_date}.csv'
    df_trades_updated.to_csv(path_cur_date)


def preprocess_acc_positions(dict_acc_positions, combine_accounts):

    fx_USD_CAD = get_fx_rate()

    list_positions = list()

    for acc, positions in dict_acc_positions.items():
        for position in positions['positions']:
            position['account'] = acc

        list_positions.extend(positions['positions'])

    df_positions = pd.DataFrame(list_positions)

    securities_cad_mask = df_positions['symbol'].str.endswith(('TO', 'VN')) & (df_positions['symbol'] != 'UCSH.U.TO')

    df_positions.loc[securities_cad_mask, 'currency'] = 'CAD'
    df_positions.loc[~securities_cad_mask, 'currency'] = 'USD'

    df_positions.loc[securities_cad_mask, 'current_market_value_CAD'] = df_positions.loc[securities_cad_mask, 'currentMarketValue']
    df_positions.loc[~securities_cad_mask, 'current_market_value_CAD'] = df_positions.loc[~securities_cad_mask, 'currentMarketValue'] * fx_USD_CAD

    df_positions.loc[securities_cad_mask, 'current_market_value_USD'] = df_positions.loc[securities_cad_mask, 'currentMarketValue'] / fx_USD_CAD
    df_positions.loc[~securities_cad_mask, 'current_market_value_USD'] = df_positions.loc[~securities_cad_mask, 'currentMarketValue']

    return df_positions


def get_acc_pos_df(combine_accounts=False):
    dict_acc_info = get_account_data()
    dict_acc_positions = get_acc_positions(dict_acc_info)
    df_positions = preprocess_acc_positions(dict_acc_positions, combine_accounts=combine_accounts)

    return df_positions


def get_qqq_pos_and_bal(acc_no):
    dict_acc_info = get_account_data()
    dict_acc_balances = get_acc_balances(list_acc_nos=[acc_no])
    dict_acc_positions = get_acc_positions(list_acc_nos=[acc_no])
    df_positions = preprocess_acc_positions(dict_acc_positions, combine_accounts=False)

    df_acc_balances = pd.DataFrame(dict_acc_balances[acc_no]['perCurrencyBalances'])
    BAL_USD = float(df_acc_balances[df_acc_balances['currency'] == 'USD']['cash'].iloc[0])

    df_sqqq = df_positions[df_positions['symbol'] == 'SQQQ']
    df_tqqq = df_positions[df_positions['symbol'] == 'TQQQ']

    n_sqqq = float(df_sqqq['openQuantity'].iloc[0] if len(df_sqqq) > 0 else 0)
    n_tqqq = float(df_tqqq['openQuantity'].iloc[0] if len(df_tqqq) > 0 else 0)

    return BAL_USD, n_sqqq, n_tqqq
