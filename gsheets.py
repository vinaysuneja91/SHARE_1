import pygsheets
import pandas as pd
# authorization


def write_to_gsheet(df, gsheet_name, sheet_no):
    gc = pygsheets.authorize(
        service_file='gstocks-api.json')
    # Create empty dataframe
    #df = pd.DataFrame()

    # Create a column
    #df['name'] = ['John', 'Steve', 'Sarah']

    # open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
    sh = gc.open(gsheet_name)

    # select the first sheet
    wks = sh[sheet_no]

    # clear sheet
    wks.clear()
    # update the first sheet with df, starting at cell B2.
    wks.set_dataframe(df, (1, 1))
