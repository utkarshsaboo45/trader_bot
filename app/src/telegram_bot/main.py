import os
import math
import json
import requests


import numpy as np
import pandas as pd
import pandas_ta as ta
import yfinance as yf

from datetime import datetime, timedelta

import sys
from pathlib import Path


ROOT_DIR = str(Path.cwd().parent.parent.parent)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


from app.src.utils.sheets_utils import (
    get_qt_token_from_sheet,
    update_qt_token_in_sheet,
)

from app.src.utils.qt_utils import (
    init_server,
    get_qqq_pos_and_bal,
)


TELEGRAM_API_KEY = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')


# --- 1. CONFIGURATION ---

RISK_PERCENTAGE_PER_TRADE = 1  # 1.0 = 100% of capital considered for risk sizing
DAILY_PRICE_DROP_EXIT_PCT = 0.05

N_PERIOD = 14
EMA_FAST_PERIOD = 50
EMA_SLOW_PERIOD = 250

EMA_BULLISH_THRESHOLD = 1.05
EMA_BEARISH_THRESHOLD = 0.95

NEUTRAL_RSI_MIN = 30
NEUTRAL_RSI_MAX = 60
OVERBOUGHT_RSI = 70

END_DATE = datetime.now().strftime('%Y-%m-%d')
TICKERS = ["QQQ", "TQQQ", "SQQQ"]
TRANSACTION_COST = 0.00

## Get and update token in sheets
token = get_qt_token_from_sheet()
refresh_token = init_server(token=token)
update_qt_token_in_sheet(refresh_token)

## Get account number from sheets
acc_no = get_qt_token_from_sheet(cell='C1')

## Get balances
BAL_USD, n_sqqq, n_tqqq = get_qqq_pos_and_bal(acc_no)

CURRENT_PORTFOLIO = {
    "SQQQ_SHARES": n_sqqq,      # Current number of SQQQ shares you hold
    "TQQQ_SHARES": n_tqqq,      # Current number of TQQQ shares you hold
    "CASH_USD": BAL_USD         # Current buying power/cash in USD in your account
}


# --- 2. INDICATOR CALCULATION ---

def calculate_indicators(df, n=N_PERIOD):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df.ta.ema(close='Close', length=EMA_FAST_PERIOD, append=True, adjust=False)
    df.ta.ema(close='Close', length=EMA_SLOW_PERIOD, append=True, adjust=False)
    df.ta.rsi(close='Close', length=n, append=True)
    df.ta.macd(close='Close', append=True)
    df.ta.atr(append=True)

    df.rename(columns={
        f'EMA_{EMA_FAST_PERIOD}': 'EMA_50',
        f'EMA_{EMA_SLOW_PERIOD}': 'EMA_250',
        f'RSI_{N_PERIOD}': 'RSI',
        'MACD_12_26_9': 'MACD',
        'MACDs_12_26_9': 'MACD_SIGNAL',
        'ATRr_14': 'ATR'}, inplace=True)

    return df.drop(columns=[col for col in df.columns if 'Adj Close' in str(col) or 'Volume' in str(col)], errors='ignore')


# --- 3. SIGNAL LOGIC ---

def generate_signal(row):
    core_cols = ['EMA_50', 'EMA_250', 'RSI', 'MACD', 'MACD_SIGNAL', 'ATR', 'Close_QQQ', 'Open_QQQ']
    if any(pd.isnull(row.get(col)) for col in core_cols):
        return "CASH"

    close = row['Close_QQQ']
    open_price = row['Open_QQQ']

    # Stop Loss
    if (close / open_price) <= (1 - DAILY_PRICE_DROP_EXIT_PCT):
        return "CASH"

    ema_50 = row['EMA_50']
    ema_250 = row['EMA_250']
    rsi_value = row['RSI']
    macd = row['MACD']
    macd_signal = row['MACD_SIGNAL']

    ema_ratio = ema_50 / ema_250
    macd_bullish = macd > macd_signal
    macd_bearish = macd < macd_signal
    is_neutral_zone = (NEUTRAL_RSI_MIN < rsi_value < NEUTRAL_RSI_MAX)
    is_overbought_zone = (rsi_value > OVERBOUGHT_RSI)

    if ema_ratio > EMA_BULLISH_THRESHOLD and is_neutral_zone and macd_bullish:
        return "TQQQ"
    elif ema_ratio < EMA_BEARISH_THRESHOLD and is_neutral_zone and macd_bearish:
        return "SQQQ"
    elif is_overbought_zone:
        return "SQQQ"
    else:
        return "CASH"


# --- 4. POSITION SIZING ---

def calculate_position_size(row, capital_available, risk_pct, trade_ticker):
    if trade_ticker == "CASH" or capital_available <= 0:
        return 0.0

    risk_amount = capital_available * risk_pct
    atr_value = row['ATR']
    if pd.isnull(atr_value) or atr_value == 0:
        return 0.0

    trade_price = row.get(f'Close_{trade_ticker}')
    if pd.isnull(trade_price) or trade_price <= 0:
        return 0.0

    stop_loss_distance_per_share = 2 * atr_value
    if stop_loss_distance_per_share <= 0:
        return 0.0

    shares_from_risk = risk_amount / stop_loss_distance_per_share
    max_shares_from_capital = capital_available / (trade_price * (1 + TRANSACTION_COST))

    position_size = math.floor(min(shares_from_risk, max_shares_from_capital)) # Use floor for whole shares
    return position_size


# --- 5. DATA & DELTA CALCULATION ---

def get_daily_delta(tickers, start_date, end_date, current_portfolio):
    # 1. Fetch Data
    all_data = pd.DataFrame()
    for ticker in tickers:
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df.columns = [col.capitalize() for col in df.columns]
            df.columns = [f'{col}_{ticker}' for col in df.columns]
            if all_data.empty: all_data = df
            else: all_data = all_data.join(df, how='outer')

    # 2. Process QQQ Indicators
    qqq_data = all_data.filter(like='_QQQ').copy()
    qqq_data.columns = [col.replace('_QQQ', '') for col in qqq_data.columns]
    qqq_data_with_indicators = calculate_indicators(qqq_data)

    # 3. Create Analysis Row
    latest_row = qqq_data_with_indicators.iloc[-1].copy()
    latest_price_tqqq = all_data[f'Close_TQQQ'].iloc[-1]
    latest_price_sqqq = all_data[f'Close_SQQQ'].iloc[-1]

    latest_row['Close_TQQQ'] = latest_price_tqqq
    latest_row['Close_SQQQ'] = latest_price_sqqq
    latest_row['Close_QQQ'] = latest_row['Close']
    latest_row['Open_QQQ'] = latest_row['Open']

    # 4. Generate Signal
    daily_signal = generate_signal(latest_row)

    # 5. Calculate Total Equity (USD)
    # Equity = Cash + (SQQQ Shares * SQQQ Price) + (TQQQ Shares * TQQQ Price)
    equity_usd = current_portfolio["CASH_USD"] + \
                 (current_portfolio["SQQQ_SHARES"] * latest_price_sqqq) + \
                 (current_portfolio["TQQQ_SHARES"] * latest_price_tqqq)

    # 6. Calculate Target Holdings
    target_tqqq = 0
    target_sqqq = 0

    if daily_signal == "TQQQ":
        target_tqqq = calculate_position_size(latest_row, equity_usd, RISK_PERCENTAGE_PER_TRADE, "TQQQ")
    elif daily_signal == "SQQQ":
        target_sqqq = calculate_position_size(latest_row, equity_usd, RISK_PERCENTAGE_PER_TRADE, "SQQQ")

    # 7. Calculate Deltas
    delta_tqqq = target_tqqq - current_portfolio["TQQQ_SHARES"]
    delta_sqqq = target_sqqq - current_portfolio["SQQQ_SHARES"]

    # Date Logic
    last_data_date = latest_row.name
    next_trading_day = last_data_date + timedelta(days=1)
    while next_trading_day.weekday() >= 5:
        next_trading_day += timedelta(days=1)

    return {
        "date": next_trading_day.strftime('%Y-%m-%d'),
        "signal": daily_signal,
        "equity": equity_usd,
        "prices": {"TQQQ": latest_price_tqqq, "SQQQ": latest_price_sqqq},
        "target": {"TQQQ": target_tqqq, "SQQQ": target_sqqq},
        "delta": {"TQQQ": delta_tqqq, "SQQQ": delta_sqqq}
    }


# -- Send response to Telegram

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_API_KEY}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    
    response = requests.post(url, json=payload)
    return response.json()


# --- Execute and Display Daily Call ---

def execute_and_send_daily_call():
    extended_start_date = (datetime.now() - timedelta(days=5 * 365 + 50)).strftime('%Y-%m-%d')

    # Run Calculation
    result = get_daily_delta(TICKERS, extended_start_date, END_DATE, CURRENT_PORTFOLIO)
    sep_dashes = "\n" + "-" * 40 + "\n"

    curr_portfolio_str = "\n".join([f"{k}: {v}" for k, v in CURRENT_PORTFOLIO.items()])
    message = f"Current portfolio: \n{curr_portfolio_str}{sep_dashes}"
    message += f"DAILY CALL FOR: {result['date']}{sep_dashes}"
    message += f"STRATEGY SIGNAL: {result['signal']}{sep_dashes}"
    message += f"Total Equity (USD): ${result['equity']:.2f}{sep_dashes}"
    message += f"Latest Prices -> TQQQ: ${result['prices']['TQQQ']:.2f} | SQQQ: ${result['prices']['SQQQ']:.2f}{sep_dashes}"
    message += "REQUIRED TRADES (DELTA):\n"

    # Print TQQQ Instructions
    if result['delta']['TQQQ'] > 0:
        message += f" [BUY]  TQQQ: {result['delta']['TQQQ']} shares\n"
    elif result['delta']['TQQQ'] < 0:
        message += f" [SELL] TQQQ: {abs(result['delta']['TQQQ'])} shares\n"
    else:
        message += f" [HOLD] TQQQ: No Change\n"

    # Print SQQQ Instructions
    if result['delta']['SQQQ'] > 0:
        message += f" [BUY]  SQQQ: {result['delta']['SQQQ']} shares"
    elif result['delta']['SQQQ'] < 0:
        message += f" [SELL] SQQQ: {abs(result['delta']['SQQQ'])} shares"
    else:
        message += f" [HOLD] SQQQ: No Change"

    return message


if __name__ == '__main__':
    daily_call = execute_and_send_daily_call()
    send_telegram(daily_call)