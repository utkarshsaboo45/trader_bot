import os
import requests

import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import numpy as np


TELEGRAM_API_KEY = os.getenv('TELEGRAM_API_KEY')
CHAT_ID = os.getenv('TELEGRAM_TRADER_CHAT_ID')


# --- 1. CONFIGURATION ---

INVESTMENT_CAPITAL_CAD = 1000.0
CAD_TO_USD_EXCHANGE_RATE = 0.73 # Example fixed rate: 1 CAD = 0.73 USD
INITIAL_CAPITAL = INVESTMENT_CAPITAL_CAD * CAD_TO_USD_EXCHANGE_RATE # Capital in USD for calculations
RISK_PERCENTAGE_PER_TRADE = 1 # 5% of capital to risk for position sizing
DAILY_PRICE_DROP_EXIT_PCT = 0.05 # 5% daily price drop to trigger a CASH signal

N_PERIOD = 14
EMA_FAST_PERIOD = 50
EMA_SLOW_PERIOD = 250

EMA_BULLISH_THRESHOLD = 1.05
EMA_BEARISH_THRESHOLD = 0.95

NEUTRAL_RSI_MIN = 30
NEUTRAL_RSI_MAX = 60
OVERBOUGHT_RSI = 70

START_DATE = (datetime.now() - timedelta(days=5 * 365 + 50)).strftime('%Y-%m-%d') # ~5 years of data for robust EMA_250
END_DATE = datetime.now().strftime('%Y-%m-%d')
TICKERS = ["QQQ", "TQQQ", "SQQQ"]
TRANSACTION_COST = 0.00 # Set transaction cost to 0


# --- 2. INDICATOR CALCULATION ---

def calculate_indicators(df, n=N_PERIOD):
    """Calculates necessary technical indicators on the QQQ dataframe."""

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df.ta.ema(close='Close', length=EMA_FAST_PERIOD, append=True, adjust=False)
    df.ta.ema(close='Close', length=EMA_SLOW_PERIOD, append=True, adjust=False)
    df.ta.rsi(close='Close', length=n, append=True)
    df.ta.macd(close='Close', append=True)
    df.ta.atr(append=True) # Default length is 14

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
    """Applies the trading logic to a single day's data row."""

    core_cols = ['EMA_50', 'EMA_250', 'RSI', 'MACD', 'MACD_SIGNAL', 'ATR', 'Close_QQQ', 'Open_QQQ']
    if any(pd.isnull(row.get(col)) for col in core_cols):
        return "CASH"

    close = row['Close_QQQ']
    open_price = row['Open_QQQ']

    # --- Daily Price Drop Stop Loss ---
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

    # --- Strategy Logic ---
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
    """
    Calculates the number of shares to trade based on available capital, risk percentage,
    and asset volatility (ATR).
    """
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

    position_size = round(min(shares_from_risk, max_shares_from_capital), 2)
    return position_size


# --- 5. Fetch and Process Data for Daily Call ---

def get_daily_call(tickers, start_date, end_date):
    all_data = pd.DataFrame()
    for ticker in tickers:
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df.columns = [col.capitalize() for col in df.columns]
            df.columns = [f'{col}_{ticker}' for col in df.columns]

            if all_data.empty:
                all_data = df
            else:
                all_data = all_data.join(df, how='outer')

    qqq_data = all_data.filter(like='_QQQ').copy()
    qqq_data.columns = [col.replace('_QQQ', '') for col in qqq_data.columns]

    qqq_data_with_indicators = calculate_indicators(qqq_data)

    latest_row = qqq_data_with_indicators.iloc[-1].copy()

    for ticker_symbol in ['TQQQ', 'SQQQ']:
        col_name = f'Close_{ticker_symbol}'
        if col_name in all_data.columns:
            latest_row[col_name] = all_data[col_name].iloc[-1]
        else:
            latest_row[col_name] = np.nan

    latest_row['Close_QQQ'] = latest_row['Close']
    latest_row['Open_QQQ'] = latest_row['Open']

    daily_call = generate_signal(latest_row)
    position_size = 0.0

    if daily_call != "CASH":
        position_size = calculate_position_size(latest_row, INITIAL_CAPITAL, RISK_PERCENTAGE_PER_TRADE, daily_call)

    # Calculate the next trading day for display
    last_data_date = latest_row.name
    next_trading_day = last_data_date + timedelta(days=1)
    while next_trading_day.weekday() >= 5: # Monday=0, Sunday=6
        next_trading_day += timedelta(days=1)

    return daily_call, next_trading_day.strftime('%Y-%m-%d'), position_size, latest_row.get('Close_TQQQ')



# -- Send response to Telegram

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_API_KEY}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    
    response = requests.post(url, json=payload)
    return response.json()


# --- Execute and Display Daily Call ---

def execute_and_send_daily_call():
    extended_start_date = (datetime.now() - timedelta(days=5 * 365 + 50)).strftime('%Y-%m-%d')

    call, date, size, tqqq_price = get_daily_call(TICKERS, extended_start_date, END_DATE)

    message = f"""Daily Call for {date}: {call}
Initial Investment (CAD): ${INVESTMENT_CAPITAL_CAD:.2f}
Effective Initial Capital (USD): ${INITIAL_CAPITAL:.2f}"""

    if call != "CASH":
        print(f"")
        message += f"Position Size: {size} shares of {call}"
    else:
        message += "\nNo position recommended (CASH)."

    if tqqq_price is not None:
        message += f"\nTQQQ Latest Close Price: ${tqqq_price:.2f}"

    return message


if __name__ == '__main__':
    daily_call = execute_and_send_daily_call()
    send_telegram(daily_call)