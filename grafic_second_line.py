from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pandas as pd
from collections import defaultdict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery
from datetime import date, timedelta
import datetime


month_name = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
              7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}



def get_table_googlesheet():
    sheet_name = f'{month_name[int(date.today().strftime("%m"))]} {int(date.today().strftime("%y"))}'
    spreadsheet_id = '1wrQ5VP8I3-6RfbX8oTCQFwh3TKSBHeHOgNBFN4TNo8E'
    range_ = f'{sheet_name}!A1:AJ100'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credential = ServiceAccountCredentials.from_json_keyfile_name('my-first-project.json', scope)
    client = gspread.authorize(credential)
    service = discovery.build('sheets', 'v4', credentials=credential).spreadsheets().values()

    result = service.get(spreadsheetId=spreadsheet_id, range=range_).execute()
    values = result.get('values', [])
    service = discovery.build('sheets', 'v4', credentials=credential)


    ###Преобразовния дата фрейма в нужный вид
    date_list = dates_of_month()
    global date_google  
    date_google = pd.DataFrame(values[6:])  #columns=values[0])
    for i in range(len(date_list)):
        date_google = date_google.rename(columns = { i+3:date_list[i]})
    date_google = date_google.rename(columns = { 2 : 'name'})
    date_google['tag'] = date_google['name'].str.split(pat="\n", expand=True).rename(columns = {2 : 'tag'})[['tag']]
    date_google['name'] = date_google['name'].str.split(pat="\n", expand=True).rename(columns = {0 : 'name'})[['name']]
    return date_google

def dates_of_month():
    today = date.today()
    first_day = date(today.year, today.month, 1)
    last_day = date(today.year, today.month + 1, 1) - timedelta(days=1)

    date_list = []
    current_day = first_day
    while current_day <= last_day:
        date_list.append(current_day)
        current_day += timedelta(days=1)

    return date_list


def get_table_googlesheet_1st_3d_lines():
    sheet_name = f'1-я и 3-я {month_name[int(date.today().strftime("%m"))]} {int(date.today().strftime("%y"))}'
    spreadsheet_id = '1wrQ5VP8I3-6RfbX8oTCQFwh3TKSBHeHOgNBFN4TNo8E'
    range_ = f'{sheet_name}!A1:AJ100'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credential = ServiceAccountCredentials.from_json_keyfile_name('my-first-project.json', scope)
    client = gspread.authorize(credential)
    service = discovery.build('sheets', 'v4', credentials=credential).spreadsheets().values()

    result = service.get(spreadsheetId=spreadsheet_id, range=range_).execute()
    values = result.get('values', [])
    service = discovery.build('sheets', 'v4', credentials=credential)


    ###Преобразовния дата фрейма в нужный вид
    date_list = dates_of_month()
    global date_google_1st_3d_line  
    date_google_1st_3d_line = pd.DataFrame(values[5:])  #columns=values[0])
    date_google_1st_3d_line.drop(index=date_google_1st_3d_line.index[0], axis=0, inplace=True)
    date_google_1st_3d_line = date_google_1st_3d_line.fillna(value='')
    
    for i in range(len(date_list)):
        date_google_1st_3d_line = date_google_1st_3d_line.rename(columns = { i+2:date_list[i]})
    
    date_google_1st_3d_line['name'] = date_google_1st_3d_line[1].str.split(pat="\n", expand=True).rename(columns = {0 : 'name'})[['name']]
    date_google_1st_3d_line['tag'] = date_google_1st_3d_line[1].str.split(pat="\n", expand=True).rename(columns = {1 : 'tag'})[['tag']]
    first_column = date_google_1st_3d_line.pop('name')
    date_google_1st_3d_line.insert(0, 'name', first_column)
    first_column = date_google_1st_3d_line.pop('tag')
    date_google_1st_3d_line.insert(1, 'tag', first_column)
    date_google_1st_3d_line.pop(1)
    
    return date_google_1st_3d_line

def who_is_on_duty_name():

    morning_duty =  datetime.datetime.now().replace(hour=7, minute=58, second=0, microsecond=0)
    evening_duty =  datetime.datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    end_of_day = datetime.datetime.now().replace(hour=22, minute=00, second=0, microsecond=0)
    duty_morning_tag = ', '.join(list(date_google['name'][date_google[date.today()] == 'ДУ']))
    duty_evening_tag = ', '.join(date_google['name'][date_google[date.today()] == 'ДВ'])
    
    if morning_duty < datetime.datetime.now() < evening_duty:
        return duty_morning_tag
    elif evening_duty < datetime.datetime.now() < end_of_day:
        return duty_evening_tag


def who_is_on_duty():

    morning_duty =  datetime.datetime.now().replace(hour=7, minute=58, second=0, microsecond=0)
    evening_duty =  datetime.datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    end_of_day = datetime.datetime.now().replace(hour=22, minute=00, second=0, microsecond=0)
    duty_morning_tag = ', '.join(list(date_google['tag'][date_google[date.today()] == 'ДУ']))
    duty_evening_tag = ', '.join(date_google['tag'][date_google[date.today()] == 'ДВ'])
    
    if morning_duty < datetime.datetime.now() < evening_duty:
        return duty_morning_tag
    elif evening_duty < datetime.datetime.now() < end_of_day:
        return duty_evening_tag


def who_is_on_duty_2d():

    morning_duty =  datetime.datetime.now().replace(hour=7, minute=58, second=0, microsecond=0)
    evening_duty =  datetime.datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    end_of_day = datetime.datetime.now().replace(hour=22, minute=00, second=0, microsecond=0)
    duty_morning_tag = ', '.join(list(date_google['tag'][date_google[date.today()] == 'ДУ']))
    duty_evening_tag = ', '.join(date_google['tag'][date_google[date.today()] == 'ДВ'])
    
    if morning_duty < datetime.datetime.now() < evening_duty:
        return duty_morning_tag
    elif evening_duty < datetime.datetime.now() < end_of_day:
        return duty_evening_tag
    
def who_is_on_duty_3d_sql():

    working_hours = datetime.datetime.now().replace(hour=8, minute=00, second=0, microsecond=0)
    morning_duty =  datetime.datetime.now().replace(hour=12, minute=00, second=0, microsecond=0)
    end_of_day = datetime.datetime.now().replace(hour=22, minute=00, second=0, microsecond=0)
    if morning_duty < datetime.datetime.now() < end_of_day:
        duty_morning_tag = ', '.join(list(date_google_1st_3d_line[(date_google_1st_3d_line[date.today()] == '10') & (date_google_1st_3d_line[0].str.contains('Sql 3-я линия'))]['tag']))
        return duty_morning_tag
    elif working_hours < datetime.datetime.now() < morning_duty and len(list(date_google_1st_3d_line[(date_google_1st_3d_line[date.today()] == '8') & (date_google_1st_3d_line[0].str.contains('Sql 3-я линия'))]['name'])) != 0:
        duty_morning_name = ', '.join(list(date_google_1st_3d_line[(date_google_1st_3d_line[date.today()] == '10') & (date_google_1st_3d_line[0].str.contains('Sql 3-я линия'))]['name']))
        sql_3d = ', '.join(list(date_google_1st_3d_line['name'][(date_google_1st_3d_line[date.today()] == '8') & (date_google_1st_3d_line[0].str.contains('Sql 3-я линия'))]))
        return f'{duty_morning_name} работает с 12:00 до 22:00, при необходимости обратитесь к {sql_3d}'
    else:
        return f'Дежурные сейчас отдыхают, но вы сможете их пойма с 8:00 до 22:00 с понедельника по пятницу и с 12:00 до 22:00 в выходные дни'

def who_is_on_duty_3d_1c():

    beginning_of_day =  datetime.datetime.now().replace(hour=00, minute=00, second=0, microsecond=0)
    midle_of_day = datetime.datetime.now().replace(hour=12, minute=00, second=0, microsecond=0)
    end_of_day = datetime.datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
    if midle_of_day <= datetime.datetime.now() <= end_of_day:
        duty_tag = ', '.join(list(date_google_1st_3d_line[(date_google_1st_3d_line[0].str.contains('1с 3-я линия день', regex=False)) & (date_google_1st_3d_line[date.today()] == '12') ]['tag']))
        return duty_tag
    elif  beginning_of_day <= datetime.datetime.now() < midle_of_day:
        duty_tag = ', '.join(list(date_google_1st_3d_line[(date_google_1st_3d_line[0].str.contains('1с 3-я линия ночь', regex=False)) & (date_google_1st_3d_line[date.today()] == '12') ]['tag']))
        return duty_tag     


def who_is_working_today():
    who_is_working =' '.join(list(date_google['tag'][date_google[date.today()] == '8']))
    return who_is_working

def second_line_tegs():
    who_is_working =' '.join(list(date_google['tag'])).lower()
    return who_is_working

def first_line_tegs():
    who_is_working =' '.join(list(date_google_1st_3d_line[date_google_1st_3d_line[0] == '1-я линия']['tag'])).lower()
    return who_is_working

def third_line_tegs():
    who_is_working =' '.join(list(date_google_1st_3d_line[date_google_1st_3d_line[0].str.contains('3-я линия', regex=False)]['tag'])).lower()
    return who_is_working

# def who_is_on_duty_3d():
#     @app.on_message(filters.chat(-1001848907467))
#     async def job2(client, message):
#         print('скрининг начался')
#         if {vars(message.from_user)["username"]} == 'Change_settings':
#             print('вижу пользователя')
#             app.stop()
#             global who_is_on_duty_3
#             who_is_on_duty_3 = '@' + vars(message.from_user)["username"]
    
#     app.run() 





date_google = get_table_googlesheet()   # при первом запуске кеширует данные.
date_google_1st_3d_line = get_table_googlesheet_1st_3d_lines()

#################### Отправка графика #####################

bot_token ='6280485351:AAFYDbrYHME0mRIvuCeBJIElOcgDuxemEs8'
api_id = 29961319
api_hash ='fba44624a98ff1f63937c8d8f8503a4b'

app = Client( "my_send_bot",
    api_id= api_id, api_hash=api_hash,
    bot_token=bot_token)


async def send_grafic():
    await app.start()
    who_is_on_weekends =', \n     '.join(list(date_google['name'][(date_google[date.today()] == 'O') | (date_google[date.today()] == '')]))
    duty_morning_name = ', \n    '.join(list(date_google['name'][date_google[date.today()] == 'ДУ']))
    duty_evening_name = ', \n    '.join(date_google['name'][date_google[date.today()] == 'ДВ'])
    if len(who_is_on_weekends) != 0:
        message = f"График работы 2-й линии: \n\n☀️Утренний дежурный☀️ \n\n     {(duty_morning_name)} \n\n🌙Вечерний дежурный🌙 \n\n    {duty_evening_name} \n\n 🏖Сегодня выходные 🏖 \n\n     {who_is_on_weekends}"
    else:
        message = f"График работы 2-й линии: \n\n☀️Утренний дежурный☀️ \n\n     {(duty_morning_name)} \n\n🌙Вечерний дежурный🌙 \n\n    {duty_evening_name} \n\n 🏖Сегодня выходные 🏖 \n\n     Работаем полным составом 😊"
    
    await app.send_message( -1001892067233, message)    #-1001848907467  1001892067233
    await app.stop()




    


scheduler = AsyncIOScheduler()
scheduler.add_job(send_grafic, "cron", hour= 10, minute= 30)     # Отправляет график работы 2-й линии
scheduler.add_job(get_table_googlesheet, "cron", hour= 8, minute= 5)  # обновляет дежурных в 8 часов
scheduler.add_job(get_table_googlesheet_1st_3d_lines, "cron", hour= 8, minute= 6) # обновляет список 1-й линии в 8 часов
    



if __name__ == '__main__':  
    scheduler.start()
    app.run() 
