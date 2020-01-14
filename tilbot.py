from __future__ import print_function
import pickle
import os.path
import json
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import ast
from collections import OrderedDict


class GoogleSheets():
    def __init__(self, cfg):
        self.config = cfg
        self.token_path = cfg['token_path']
        self.connect()
        return

    def connect(self):
        self.cred = None
        if os.path.exists(self.token_path):
            with open(self.token_path, 'rb') as token:
                self.cred = pickle.load(token)
        if not self.cred or not self.cred.valid:
            if self.cred and self.cred.expired and self.cred.refresh_token:
                self.cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config['credentials_path'],
                    self.config['scope'])
                self.cred = flow.run_local_server(port=0)
            with open(self.token_path, 'wb') as token:
                pickle.dump(self.cred, token)

    def log_http_error(self, sheet_id):
        print('Something went wrong during connection to GDrive')
        print('Ensure the following URL is valid:')
        print('https://docs.google.com/spreadsheets/d/' + sheet_id)
        exit()

    def column_from_table(self, table, column_name):
        column_list = []
        index = table[0].index(column_name)
        for row in table[1:]:
            column_list.append(row[index])
        return column_list

    def get_dataframe_from_table(self, sheet_id):
        sheet = build('sheets', 'v4', credentials=self.cred).spreadsheets()
        try:
            result = sheet.values().get(
                spreadsheetId=sheet_id,
                range=self.config['range']).execute()
            table = result.get('values', [])
            if not table:
                raise Exception
        except:
            self.log_http_error(sheet_id)
        id_list = self.column_from_table(table, 'id')
        return pd.DataFrame(table[1:], columns=table[0], index=id_list)


def main(sheet_config):
    sheets = GoogleSheets(sheet_config)
    table_id = sheet_config['id']
    tiller = sheets.get_dataframe_from_table(table_id)
    for id, transaction in tiller.iterrows():
        print(id, transaction.amount)


if __name__ == '__main__':
    with open('sheets.cfg', 'r') as config_file:
        sheet_config = json.load(config_file)
    main(sheet_config)
