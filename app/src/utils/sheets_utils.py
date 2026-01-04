import os
import json
import gspread
from google.oauth2.service_account import Credentials

# PATH_SERVICE_ACCOUNT_FILE = '../../configs/creds_gcp_qt.json'
# PATH_CONFIG = '../../configs/api_keys.json'

# with open(PATH_CONFIG, 'r') as file:
#     config = json.load(file)

# WORKBOOK_ID = config['id_sheet_qt_portfolio']
WORKBOOK_ID = os.getenv('ID_SHEET_QT_PORTFOLIO')


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

SHEET_HOLDINGS = None
WORKBOOK = None


def init_workbook():
    global SHEET_HOLDINGS

    info = json.loads(os.getenv('SERVICE_ACCOUNT_JSON'))
    creds = Credentials.from_service_account_info(
        info, 
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )

    # creds = Credentials.from_service_account_file(PATH_SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    client = gspread.authorize(creds)

    WORKBOOK = client.open_by_key(WORKBOOK_ID)
    SHEET_HOLDINGS = WORKBOOK.worksheet('holdings')


def get_qt_token_from_sheet(cell='B1'):
    if not SHEET_HOLDINGS:
        init_workbook()

    qt_token = SHEET_HOLDINGS.acell(cell).value
    return qt_token


def update_qt_token_in_sheet(refresh_token, cell='B1'):
    if not SHEET_HOLDINGS:
        init_workbook()

    SHEET_HOLDINGS.update_acell(cell, refresh_token)


def update_sheets_with_data(df_positions, cell='A3'):

    if not SHEET_HOLDINGS:
        init_workbook()

    try:
        output_data = [df_positions.columns.values.tolist()] + df_positions.values.tolist()
        SHEET_HOLDINGS.update(cell, output_data)
        return "Success: Sheet updated!", 200
    except Exception as e:
        return f"Sheets Error: {str(e)}", 500
