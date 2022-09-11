import twstock
import time
import requests

def LINE_Notify(token, msg):
    headers = {
     "Authorization": "Bearer " + token,
     "Content-Type" : "application/x-www-form-urlencoded"
    }

    payload = {'message': msg}

    notify = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)

    return notify.status_code

def sendline(mode, realprice, counterLine, token):
    print('台積電目前股價：' + str(realprice))

    if mode == 1:
        message = '現在台積電股價：' + str(realprice) + ' SELL it!'
    else:
        message = '現在台積電股價：' + str(realprice) + ' BUY it!'

    code = LINE_Notify(token, message)

    if code == 200:
        counterLine = counterLine + 1
        print('Line Message No.' )
        print( counterLine)
    else:
        print('Sent Line message FAILURE')
    return counterLine

token = 'your_token_id'
counterLine = 0
counterError = 0

realdata = twstock.realtime.get('2330')
print(realdata['realtime']['latest_trade_price'])

while True:
    realdata = twstock.realtime.get('2330')
    if realdata['success']:
        realprice = realdata['realtime']['latest_trade_price']

        if float(realprice) >= 610:
            counterLine = sendline(1, realprice, counterLine, token)
        elif float(realprice) <= 530:
            counterLine = sendline(2, realprice, counterLine, token)
        if counterLine >= 3 :
            print('Stop!')
            break
    else:
        print('Loading twstock is Error, error message :' + realdata['rtmessage'])
        counterError = counterError + 1
        if counterError >= 3:
            print('Stop!!!')
            break
    #for i in range(300):
        #time.sleep()