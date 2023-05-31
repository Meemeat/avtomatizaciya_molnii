import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from decimal import Decimal
from datetime import datetime, timedelta
from grafic_second_line import get_table_googlesheet, dates_of_month, who_is_on_duty, who_is_on_duty_name
from apscheduler.schedulers.asyncio import AsyncIOScheduler

app = Client(
    name="parsing2",
    api_id='23786606',
    api_hash='e792793e75ed923353dc543b7f3c72bb')

scheduler = AsyncIOScheduler()
# scheduler.add_job(get_table_googlesheet, "cron", hour= 00, minute= 5)  # обновляет дежурных в 00 часов
teg=[]
# scheduler.add_job(teg.append(get_table_googlesheet()), "cron", hour= 00, minute= 5)#, scheduler.add_job(get_table_googlesheet, "cron", hour= 15, minute= 5) #get_table_googlesheet()
date_google = get_table_googlesheet()
name_dejurnogo = who_is_on_duty_name()
 


cashe2 = {}

def number_of_week():
    date1 = datetime(2009, 5, 11)
    date2 = datetime.now()
    days = abs(date1 - date2).days
    return days // 7 + 1


def new_last_raw_googlesheet(molniya):
    spreadsheet_id = '1eyI4W89V4FLPkz-XoytyfVCbzPECKSlPMnJANdxxAyw'
    range_ = 'Алармы!A1:J'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credential = ServiceAccountCredentials.from_json_keyfile_name('my-first-project.json', scope)
    client = gspread.authorize(credential)
    service = discovery.build('sheets', 'v4', credentials=credential).spreadsheets().values()
    result = service.get(spreadsheetId=spreadsheet_id, range=range_).execute()
    values = result.get('values', [])
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet("Алармы")
    df = pd.DataFrame(values[1:], columns=values[0][:10])
    # print(df['Номер молнии'])
    #raw_number = df[df['Номер молнии'] != ''].index.tolist()[-1] +3 
    nomer_molnii = int(df['Номер молнии'][0]) + 1
    sheet.insert_row([nomer_molnii, (datetime.now()).strftime("%Y-%m-%d") ,(datetime.now()).strftime("%H:%M:%S"),'', '','','',name_dejurnogo,molniya,''], index=2)
    

  


def update_last_raw_googlesheet(raw_number):
    spreadsheet_id = '1eyI4W89V4FLPkz-XoytyfVCbzPECKSlPMnJANdxxAyw'
    #range_ = 'Алармы!A1:J'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credential = ServiceAccountCredentials.from_json_keyfile_name('my-first-project.json', scope)
    client = gspread.authorize(credential)
    #service = discovery.build('sheets', 'v4', credentials=credential).spreadsheets().values()
    #result = service.get(spreadsheetId=spreadsheet_id, range=range_).execute()
    #values = result.get('values', [])
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet("Алармы")
    data = [(datetime.now()).strftime("%H:%M:%S"), f'=D{raw_number}- C{raw_number}']
    range_to_update = f'D{raw_number}:F{raw_number}'
    sheet.update(range_to_update, [data],
                  value_input_option='USER_ENTERED')

