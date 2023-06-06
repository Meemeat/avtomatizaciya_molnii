from pyrogram import Client, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from grafic_second_line import  get_table_googlesheet, who_is_on_duty_2d, dates_of_month, second_line_tegs, send_grafic,  who_is_on_duty_3d_sql2
from avtomat_molnii import new_last_raw_googlesheet, update_last_raw_googlesheet
import datetime

# app = Client(
#     name="parsing2",
#     api_id='23786606',
#     api_hash='e792793e75ed923353dc543b7f3c72bb')


app = Client(
    name="parsing2",
    api_id='23786606',
    api_hash='e792793e75ed923353dc543b7f3c72bb')


"""config"""

chat_to_listen = -808740151                              #chats in which bot is working
chats = [-1001848907467,-1001704351319]              #chats in which bot is sending message to inform  business users 
chats_ban_reliz  = [-922422558]                           #chats in which bot is sending message to inform  developers      
chat_podderjka = [-1001704351319]   
chat_reanimacia = [-1001848907467]
textalarm = '‼️'  
textalarm2 = 'наблюд'                                     #message to start lightning
textalarm_serv = 'сервер'
textinfo = 'дляинформ'                                      #message to inform users
tegi_vechernih_dej = ['imaryasha','psk1s']
text_molniya = '⚠️**На текущий момент решается молния, любые релизы запрещены!**'
text_molniya_zakrytya = '✅**На текущий момент молния закрыта, далее релизы по согласованию**'
text_com = 'Добрый день, у нас проблема с ________, (подробное описание проблемы). Предварительная массовость  _____. Подключитесь, пожалуйста, к решению проблемы. '
text_zoom = f'Ссылка на зум по проблеме https://us02web.zoom.us/j/3186943687?pwd=cEpzK0xpRVEzWVBJKzhVbDVSeXVZZz09.@Ivan1V1Stepin, {who_is_on_duty_3d_sql2()} подключитесь пожалуйста'
zoom_message = 'Ссылка на зум по проблеме'

"""start of the program"""
id_cashe={}
cashe = []
second = second_line_tegs().replace(' ', '').split('@')
for i in range(len(tegi_vechernih_dej)):
    second.append(tegi_vechernih_dej[i])


def update_tegs():
    global second 
    second = second_line_tegs().replace(' ', '').lower().split('@')

def id_cashe_is_null():
    global id_cashe
    id_cashe = {}

async def copy_and_send_message(chats, chat_to_listen, message_id1):
    for chat in chats:
            sent_message  = await app.copy_message( chat_id= chat ,from_chat_id= chat_to_listen, message_id = message_id1)
            global id_cashe
            id_cashe[message_id1][message_id1].append(sent_message.id)

async def send_messages(chats, text):
    for chat in chats:
            sent_message  = await app.send_message( chat_id= chat ,text= text)


cashe2 ={}


@app.on_message(filters.chat(chat_to_listen)) 
async def new_channel_post(client, message):
    """"Первый иф проверяет отправлять или нет коммуникацию во все чаты, второй иф проверяет на момент является ли сообщение 'реплаем' если да, то перенаправляет в другие чаты"""
    
    match_alarm = message.text.replace(' ', '').lower().find(textalarm)
    match_alarm2 = message.text.replace(' ', '').lower().find(textalarm2)
    match_info = message.text.replace(' ', '').lower().find(textinfo)
    match_alarm_serv = message.text.replace(' ', '').lower().find(textalarm_serv)
   

    if match_alarm != -1 and message.from_user.username.lower() in second and match_alarm_serv != -1:
        id_cashe[message.id] = {}
        id_cashe[message.id].setdefault( message.id, [])
        await copy_and_send_message(chat_podderjka, chat_to_listen, message_id1 = message.id)
        if len(id_cashe) <= 1:
            await send_messages( chats_ban_reliz, text_molniya)
        #await send_messages( chat_to_listen, text_molniya)
    elif match_alarm != -1 and match_alarm2 != -1 and message.from_user.username.lower() in ['igntbc','jekabotstethemov']:
        id_cashe[message.id] = {}
        id_cashe[message.id].setdefault( message.id, [])
        await copy_and_send_message(chat_podderjka, chat_to_listen, message_id1 = message.id)
        if len(id_cashe) <= 1:
            await send_messages( chats_ban_reliz, text_molniya)
        await send_messages(chat_podderjka, text_com)    
        # for chat in chats:
        #     sent_messagee  = await app.copy_message( chat_id= chat ,from_chat_id= chat_to_listen, message_id = message.id )
        #     #print(sent_messagee)
        #     id_cashe[message.id][message.id].append(sent_messagee.id)
        info = message.text.replace('‼️', '')
        new_last_raw_googlesheet(info)
        cashe.append(message.id)
        async def replay():
            for i in range(len(cashe)):
                for m in list(id_cashe.keys()):
                    sent_message = await client.send_message(
                    chat_id= chat_to_listen,
                    text= 'На данный момент проблема в работе, специалисты занимаются решением',
                    reply_to_message_id=cashe[i])
                    uid = sent_message.id
                    id_cashe[m].setdefault(uid, [])
                    #id_cashe[m][uid].append(sent_message.id)
                    print(id_cashe[m].keys(), 1)
            for z in range(len(chat_podderjka)):
                for n in range(len(id_cashe.keys())):
                    for m in list(id_cashe.keys()):
                        sent_message = await client.send_message(
                        chat_id=chat_podderjka[z],
                        text= 'На данный момент проблема в работе, специалисты занимаются решением',
                        reply_to_message_id=id_cashe[list(id_cashe.keys())[n]][list(id_cashe.keys())[n]][z])
                        id_cashe[m][uid].append(sent_message.id)
        
        async def zoom_molniya():
            await send_messages(chat_podderjka, text_zoom)#chat_reanimacia
            await send_messages(chat_reanimacia, 'Ссылка на зум по проблеме https://us02web.zoom.us/j/3186943687?pwd=cEpzK0xpRVEzWVBJKzhVbDVSeXVZZz09.@Здесь будут теги DBA/DevOps, подключитесь пожалуйста')


            
        plan_datetime = datetime.datetime.now() + datetime.timedelta(seconds=20)
        scheduler.add_job(replay, 'interval',  seconds=40 , id=f'message_repl')
        scheduler.add_job(zoom_molniya, 'date',  run_date=plan_datetime , id=f'message_zoom')

    
    elif  match_info != -1 and message.from_user.username.lower() =='igntbc':
            for chat in chat_podderjka:
                sent_message  = await app.copy_message( chat_id= chat ,from_chat_id= chat_to_listen, message_id = message.id )
 
  
    if message.reply_to_message:# and message.from_user.username.lower() in ['jekabotstethemov','igntbc']:
        for i in list(id_cashe.keys()):
            if message.reply_to_message_id in id_cashe[i].keys(): 
                print(id_cashe[i].keys(), 2)
                print(id_cashe)
                id_cashe[i].setdefault( message.id, [])
                for z in range(len(chat_podderjka)):
                    sent_message = await client.send_message(
                    chat_id=chat_podderjka[z],
                    text=message.text,
                    reply_to_message_id=id_cashe[i][message.reply_to_message_id][z])

                    id_cashe[i][message.id].append(sent_message.id)


                match1 = message.text.replace(' ', '').lower().find('✅')
                if match1 != -1:

                    
                    number_row = 2 + len(cashe) - (cashe.index(i)+1)
                    update_last_raw_googlesheet(number_row)
                    await app.send_message(-1001704351319, f'{who_is_on_duty_2d()}, молния завершена, не забудь запросить отчет по форме https://docs.google.com/document/d/1f1PZwOPqmhXlnCxcF5gRU_TqXdz3tXc_nHa6DbsYYzo/')
                    del id_cashe[i]
                    if len(id_cashe)==0:
                        cashe.clear()
                    scheduler.remove_job(f'message_repl')
                       
                
                if match1 != -1 and len(id_cashe) == 0:
                    await send_messages( chats_ban_reliz, text_molniya_zakrytya)  




scheduler = AsyncIOScheduler()
scheduler.add_job(id_cashe_is_null, "cron", hour= 5, minute= 30) 
scheduler.add_job(update_tegs, "cron", hour= 00, minute= 5)

if __name__ == '__main__':
    scheduler.start()
    app.run()


# i ={3641: {3641: [3642, 2959]}, 3643: {3643: [3644, 2960]}}
# for n in (range(len(i.keys()))):
#     print(n)

# print(list(i.keys()))
# print(i[list(i.keys())[0]][list(i.keys())[0]])

# chats = [-1001848907467, -922422558] 
# for chat in chats:
#     print(chat)

# n = {3987: {3987: [3988, 3120, 1405, 3989, 3993], 3991: [3992, 3122, 1407]}}
# print(n[3987][3987])
# print(n[3987], n[3991])

# import datetime
# from apscheduler.schedulers.asyncio import AsyncIOScheduler

# def job():
#     current_datetime = datetime.now()
#     print(current_datetime)


# plan_datetime = datetime.datetime.now() + datetime.timedelta(seconds=5)
# scheduler = AsyncIOScheduler()
# scheduler.add_job(job, 'date', run_date=plan_datetime, id='job_3')
# if __name__ == '__main__':
#     scheduler.start()
