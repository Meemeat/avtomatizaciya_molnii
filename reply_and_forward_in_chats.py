from pyrogram import Client, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from grafic_second_line import  get_table_googlesheet, who_is_on_duty_2d, dates_of_month, second_line_tegs, send_grafic
from avtomat_molnii import new_last_raw_googlesheet, update_last_raw_googlesheet


# app = Client(
#     name="parsing2",
#     api_id='23786606',
#     api_hash='e792793e75ed923353dc543b7f3c72bb')


app = Client(
    name="my_account",
    api_id='29961319',
    api_hash='fba44624a98ff1f63937c8d8f8503a4b'
    )



"""config"""

chat_to_listen = -1001252052260                              #chats in which bot is working
chats = [-1001848907467]              #chats in which bot is sending message to inform  business users
chats_ban_reliz  = [-1001848907467]                           #chats in which bot is sending message to inform  developers         
textalarm = '‼️наблюд'                                       #message to start lightning
textalarm_serv = 'сервер'
textinfo = 'дляинформ'                                      #message to inform users
tegi_vechernih_dej = ['imaryasha','psk1s']

"""start of the program"""
id_cashe={}
cashe = []
second = second_line_tegs().replace(' ', '').split('@')
for i in range(len(tegi_vechernih_dej)):
    second.append(tegi_vechernih_dej[i])
#print(second)



def update_tegs():
    global second 
    second = second_line_tegs().replace(' ', '').lower().split('@')

def id_cashe_is_null():
    global id_cashe
    id_cashe = {}
cashe2 ={}
@app.on_message(filters.chat(chat_to_listen)) 
async def new_channel_post(client, message):
    """"Первый иф проверяет отправлять или нет коммуникацию во все чаты, второй иф проверяет на момент является ли сообщение 'реплаем' если да, то перенаправляет в другие чаты"""
    
    match_alarm = message.text.replace(' ', '').lower().find(textalarm)
    match_info = message.text.replace(' ', '').lower().find(textinfo)

    if match_alarm != -1 and message.from_user.username.lower() in second:
        id_cashe[message.id] = {}
        id_cashe[message.id].setdefault( message.id, [])
        for chat in chats:
            sent_message  = await app.copy_message( chat_id= chat ,from_chat_id= chat_to_listen, message_id = message.id )
            id_cashe[message.id][message.id].append(sent_message.id)
        info = message.text.replace('‼️', '')
        new_last_raw_googlesheet(info)
        cashe.append(message.id)


    
    elif  match_info != -1 and message.from_user.username.lower() in second:
            for chat in chats:
                sent_message  = await app.copy_message( chat_id= chat ,from_chat_id= chat_to_listen, message_id = message.id )
                
            
    if message.reply_to_message and message.from_user.username.lower() in second:
        for i in list(id_cashe.keys()):
            if message.reply_to_message_id in id_cashe[i].keys(): 
                id_cashe[i].setdefault( message.id, [])
                for z in range(len(chats)):
                    sent_message = await client.send_message(
                    chat_id=chats[z],
                    text=message.text,
                    reply_to_message_id=id_cashe[i][message.reply_to_message_id][z])

                    id_cashe[i][message.id].append(sent_message.id)

                match1 = message.text.replace(' ', '').lower().find('✅')
                if match1 != -1:
                    

                    number_row = 2 + len(cashe) - (cashe.index(i)+1)
                    update_last_raw_googlesheet(number_row)
                    # for m in range(len(id_cashe.keys())):
                    #     number_row = 2+int(len(id_cashe.keys())/2)-int(m/2)
                    #     update_last_raw_googlesheet(number_row) 
                    del id_cashe[i]
                    if len(id_cashe)==0:
                        cashe.clear()
                    
                       
                
                break




scheduler = AsyncIOScheduler()
scheduler.add_job(id_cashe_is_null, "cron", hour= 5, minute= 30) 
scheduler.add_job(update_tegs, "cron", hour= 00, minute= 5)

if __name__ == '__main__':
    scheduler.start()
    app.run()


