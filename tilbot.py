from __future__ import print_function
import os, re
import pickle
import json
import pandas as pd
import threading
from money_parser import price_str
from time import sleep
from twilio.rest import Client as twilio_api
from googleapiclient.discovery import build as connect_to_sheet
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

class TableUtils:
    def __init__(self, data):
        self.df = data
        self.parse_transaction_amounts()

    def parse_money(self, amount_str):
        if type(amount_str) is str:
            amount_str = float(price_str(amount_str.replace('$','')))
        return amount_str

    def parse_transaction_amounts(self):
        self.df['amount'] = self.df['amount'].apply(self.parse_money)

    def get_rows_with_column_str(self, column, value):
        # return all rows that contain a keyword in a specific column
        return self.df[column.str.contains(value,case=False)]

    def sum_specific_account(self, account_name):
        account = self.get_rows_with_column_str(self.df.account, account_name)
        return account['amount'].sum()

    def sum_all_accounts(self):
        return self.df['amount'].sum()

    def remove_rows_with_column_str(self, column, value):
        # Take out rows matching a certain column value, return those rows
        to_remove = self.get_rows_with_column_str(column, value)
        self.df.drop(list(to_remove.index), inplace=True)
        return to_remove

class ChronJob:
    def __init__(self, data, interval, retries=1, sms_client=None):
        self.data         = data,
        self.interval     = interval
        self.retries      = retries
        self.sms_client   = sms_client
        self.table_utils  = TableUtils(data)

    def handle_error(self, err, job_name):
        print('Error occurred for job {}'.format(job_name))
        print(err)
        if self.retries > 0:
            self.retries -= 1
            print('Restarting...')
            return True
        print('Out of retries. Exiting...')
        return False

    def sleep_over_interval(self):
        sleep(self.interval*60*60)


class SMSReport(ChronJob):

    def start(self):
        print('SMS Report executing.')
        try:
            while True:
                self._run()
                self.sleep_over_interval()
        except Exception as err:
            if self.handle_error(err, 'SMSReport'):
                self.start()
            else:
                raise err

    def _run(self):
        extract_rows = self.table_utils.remove_rows_with_column_str
        transfers = extract_rows(self.table_utils.df.description, 'transfer')
        savings_transfers = extract_rows(self.table_utils.df.description, 'goldman sachs')

        account_total = self.table_utils.sum_specific_account
        paypal_total = account_total('PayPal')
        wellsfargo_total = account_total('Wells Fargo Checking')
        amex_total = account_total('Amex')
        print('paypal\t: {}'.format(paypal_total))
        print('wellsfargo\t: {}'.format(wellsfargo_total))
        print('amex\t: {}'.format(amex_total))
        
        self.sms_client.send_message('Heres a test')


class GoogleSheets():
    def __init__(self, cfg):
        self.config = cfg
        self.connect(cfg['token_path'])
        return

    def connect(self, token_path):
        self.cred = None
        if os.path.exists(token_path):
            with open(token_path, 'rb') as token:
                self.cred = pickle.load(token)
        if not self.cred or not self.cred.valid:
            if self.cred and self.cred.expired and self.cred.refresh_token:
                try:
                    self.cred.refresh(Request())
                except:
                    print('Token for Google Sheets has expired. Reverifying...')
                    os.remove(token_path)
                    self.connect(token_path)
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config['credentials_path'],
                    self.config['scope'])
                self.cred = flow.run_local_server(port=0)
            with open(token_path, 'wb') as token:
                pickle.dump(self.cred, token)

    def log_connection_error(self, sheet_id):
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

    def get_dataframe_from_table(self, sheet_id, cell_range, index=None):
        sheet = connect_to_sheet(
            'sheets', 'v4', credentials=self.cred).spreadsheets()
        try:
            result = sheet.values().get(spreadsheetId=sheet_id, range=cell_range).execute()
            table = result.get('values', [])
            if not table:
                raise Exception
        except:
            self.log_connection_error(sheet_id)

        if index:
            if type(index) is not str:
                print('Bad index format. {} must be string'.format(index))
            else:
                for i, column_name in enumerate(table[0]):
                    table[0][i] = column_name.replace(' ','').lower().replace(index,'id')
                id_list = self.column_from_table(table, 'id')
                df = pd.DataFrame(table[1:], columns=table[0], index=id_list)
        else:
            df = pd.DataFrame(table[1:], columns=table[0])
        return df


class TwilioClient():
    def __init__(self, cfg):
        self.config = cfg
        self.connect()

    def connect(self):
        self.client = twilio_api(self.config['sid'], self.config['token'])

    def send_message(self, body):
        self.client.messages.create(
            to=self.config['client_phone'],
            from_=self.config['host_phone'],
            body=body)


def main(sheet_config, twilio_config, chron_config):
    # Connect to Google Sheets
    sheets = GoogleSheets(sheet_config)
    table_id = sheet_config['id']
    transaction_cells = sheet_config['transaction_cells']
    balance_cells = sheet_config['balance_cells']
    tiller_data  = sheets.get_dataframe_from_table(table_id, transaction_cells, index='transactionid')
    tiller_balance = sheets.get_dataframe_from_table(table_id, balance_cells, index='account')
    # Connect to Twilio SMS
    sms = TwilioClient(twilio_config)
    job1 = SMSReport(tiller_data, chron_config['interval'],sms_client=sms)
    job1.start()
    # # creating thread
    # t1 = threading.Thread(target=print_one, args=(10,))
    # t2 = threading.Thread(target=print_two, args=(10,))
    # # starting threads
    # t1.start()
    # t2.start()

    # # wait until threads are completely executed
    # t1.join()
    # t2.join()

    print("Done!")


if __name__ == '__main__':
    with open('sheets.cfg', 'r') as sheets_config_file:
        sheet_config = json.load(sheets_config_file)
    with open('twilio.cfg', 'r') as twilio_config_file:
        twilio_config = json.load(twilio_config_file)
    with open('chron.cfg', 'r') as chron_config_file:
        chron_config = json.load(chron_config_file)
    main(sheet_config, twilio_config, chron_config)
