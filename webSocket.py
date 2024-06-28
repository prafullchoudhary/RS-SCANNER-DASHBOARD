# root='wss://ws.kite.trade' 
import kiteapp as kt
import pandas as pd
from time import sleep
from datetime import datetime
# with open('enctoken.txt', 'r') as rd:
#     token = rd.read()

kite= kt.KiteApp("Prafull","VD6317","rCa0ikUWW8DB9kBHK+nhI/206yhm7ozaPF5SwjIqYlQBarLyFQpIzt0K+dheYJ7mRP38uJ8bIRwGLRYto+mF9+ik5zXREodqyyyyTfh91cTgMHSAROHdJg==")

kws = kite.kws() #For Websocket

sectors=pd.read_csv("tradable.csv")
# inst=pd.DataFrame(kite.instruments(exchange='NSE'))
# # stocks=inst[inst['tradingsymbol'].isin(sectors['NSE_Symbol'])]['instrument_token'].to_list()
# # sectors['instrument_token'] = pd.merge(sectors['NSE_Symbol'], inst[['instrument_token','tradingsymbol']], left_on='NSE_Symbol', right_on='tradingsymbol', how='inner', suffixes=('_sectors', '_inst'))['instrument_token']
# sectors=pd.merge(sectors, inst[['instrument_token','tradingsymbol']], left_on='NSE_Symbol', right_on='tradingsymbol', how='inner').drop('tradingsymbol', axis=1)
# time1=datetime.now()
def on_ticks(ws, ticks):
    # global time1
    # pass
    ticks.to_csv("ticks.csv",index=False)
    # time2=datetime.now()
    # print(time2-time1)
    # time1=time2

def on_connect(ws, response):
    ws.subscribe(sectors['instrument_token'].to_list())
    ws.set_mode(ws.MODE_LTP,sectors['instrument_token'].to_list()) # MODE_FULL , MODE_QUOTE MODE_LTP

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.connect(threaded = True)
sleep(300)
kws.close()
