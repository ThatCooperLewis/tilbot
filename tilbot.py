from __future__ import print_function
import os
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

class TableMath:
    def __init__(self, data):
        self.df = data

    def parse_money(self, amount_str):
        return float(price_str(amount_str.replace('$','')))

    def parse_transaction_amounts(self):
        for index, row in self.df.iterrows():
            print('INDEX: {}'.format(index))
            print(row['amount'])
            amount = self.parse_money(row['amount'])
            # print(amount)
            self.df[index]['amount'] = amount


class TableAggregation(TableMath):

    def sum_account(self):
        self.parse_transaction_amounts()
        accounts = self.df[self.df.account.str.contains('Blue Cash Everyday',case=False)]
        # parsed_transactions = self.parse_row
        sum = accounts.amount.sum()
        print(sum)

class ChronJob:
    def __init__(self, data, interval, retries=1, sms_client=None):
        self.data       = data,
        self.interval   = interval
        self.retries    = retries
        self.sms_client = sms_client
        self.table_agg  = TableAggregation(data)

    def handle_error(self, err, job_name):
        print('Error occurred for job {}'.format(job_name))
        print(err)
        if self.retries > 0:
            self.retries -= 1
            print('Restarting...')
            return True
        print('Out of retries. Exiting...')
        exit()

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

    def _run(self):
        print('SMS report executing')
        self.table_agg.sum_account()
        # self.sms_client.send_message('Heres a test you fucking bitch')


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
        index = table[0].index(column_name) - 1
        print(index)
        for row in table[1:]:
            try:
                column_list.append(row[index])
            except:
                print(row)
        return column_list

    def get_dataframe_from_table(self, sheet_id):
        sheet = connect_to_sheet(
            'sheets', 'v4', credentials=self.cred).spreadsheets()
        try:
            result = sheet.values().get(
                spreadsheetId=sheet_id,
                range=self.config['range']).execute()
            table = result.get('values', [])
            if not table:
                raise Exception
        except:
            self.log_connection_error(sheet_id)
        id_list = self.column_from_table(table, 'id')
        return pd.DataFrame(table[1:], columns=table[0], index=id_list)


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


# def print_one(task, interval, data):
#     i = 0
#     while i < 5:
#         print(num)
#         sleep(4)
#         i += 1
#     print('one done')


# def print_two(num):
#     i = 0
#     while i < 7:
#         print(num)
#         sleep(3)
#         i += 1
#     print('two done')


def main(sheet_config, twilio_config, chron_config):
    # Connect to Google Sheets
    sheets = GoogleSheets(sheet_config)
    table_id = sheet_config['id']
    tiller_data  = sheets.get_dataframe_from_table(table_id)
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
