import pandas as pd
import csv
import util

# for more than one sheet
# sheet_id = '1TtuU7wu05m-Jefzwyj4NAYzr4XSYA3NHV44LvMgh0ko'
# sheet_name = “mock_data”
# url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

def read_sheet():
    """
    """
    sheet_url = r'https://docs.google.com/spreadsheets/d/1TtuU7wu05m-Jefzwyj4NAYzr4XSYA3NHV44LvMgh0ko/edit#gid=0'
    url_1 = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')

    mock = pd.read_csv(url_1)
    util.insert_sql(mock, 'mock')

