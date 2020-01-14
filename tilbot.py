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
        # Use existing token if available
        if os.path.exists(self.token_path):
            with open(self.token_path, 'rb') as token:
                self.cred = pickle.load(token)
        # Re-auth if no token
        if not self.cred or not self.cred.valid:
            if self.cred and self.cred.expired and self.cred.refresh_token:
                self.cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config['credentials_path'],
                    self.config['scope'])
                self.cred = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, 'wb') as token:
                pickle.dump(self.cred, token)

    def get_dataframe(self, sheet_id):
        sheet = build('sheets', 'v4', credentials=self.cred).spreadsheets()
        try:
            result = sheet.values().get(spreadsheetId=sheet_id,
                                        range=self.config['range']).execute()
            table = result.get('values', [])
            if not table:
                raise
        except:
            print('Provided spreadsheet not accessible.')
            print('Ensure the following URL is valid:')
            print('https://docs.google.com/spreadsheets/d/' + sheet_id)
            exit()
        else:
            id_list = []
            id_index = table[0].index('id')
            for row in table[1:]:
                id_list.append(row[id_index])
            return pd.DataFrame(table[1:], columns=table[0], index=id_list)


def main(sheet_config):
    sheets = GoogleSheets(sheet_config)
    # Create pandas dataframe from GDrive sheet
    tiller = sheets.get_dataframe(sheet_config['id'])
    for id, transaction in tiller.iterrows():
        print(id, transaction.amount)


if __name__ == '__main__':
    with open('sheets.cfg', 'r') as config_file:
        sheet_config = json.load(config_file)
    main(sheet_config)
