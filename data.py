from datetime import datetime,timedelta
import config
import pandas as pd
import kiteapp as kt
import os
from time import sleep
import numpy as np
import concurrent.futures
import requests
import json

sectors=pd.DataFrame()
previous_day=datetime.now()-timedelta(days=1)
# previous_hour=datetime.now()-timedelta(hours=1)

def stock_series(stock):
    return [f'{stock}-BE',f'{stock}-BZ']

def calculate_sector_df(df,no_of_s,st):
    sd=0
    df[st]=0
    for l in range(-no_of_s,0):
        df[st]+=df.iloc[:, l-1]*abs(l)
        sd+=abs(l)
    df[st]=round((df[st]/sd),2)
    return df[st]

def calculate_sector_volume(df,no_of_s):
    df['volume']=0
    for l in range(-no_of_s,0):
        df['volume']+=df.iloc[:, l-1]
    return df['volume']

def sector_ltp():
    def calc_ltp(ltp_col,it_sec):
        if it_sec!='Nifty_Indices':
            global table_df
            sd=0
            sec_ltp=0
            for l in range(-len(ltp_col),0):
                sec_ltp+=ltp_col.iloc[l]*abs(l)
                sd+=abs(l)
            index=table_df[table_df['instrument_token'] == it_sec].index[0]
            table_df.at[index, 'LTP']=round((sec_ltp/sd),2)
    global sectors
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        futures1=[executor.submit(calc_ltp,sectors[sectors['Sector']==sector]['LTP'],sector) for sector in sectors['Sector'].unique()]
    executor.shutdown(wait=True)
    table_df.to_csv("table_df.csv",index=False)
    # print(datetime.now().time())

def df_merge(df1,df2):
    return pd.merge(df1, df2, on='instrument_token', how='left')

def fill_ltp(c,x,y):
    return np.where(c, x, y)

def on_ticks(ws, ticks):
    # print(datetime.now().time())
    global sectors
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result1 = executor.submit(df_merge,sectors[['instrument_token','LTP']], ticks)
        result2 = executor.submit(df_merge,table_df[['instrument_token','LTP']], ticks)
        result1=result1.result()
        result2=result2.result()

        result3=executor.submit(fill_ltp,result1['LTP_y'].isna(), result1['LTP_x'], result1['LTP_y'])
        result4=executor.submit(fill_ltp,result2['LTP_y'].isna(), result2['LTP_x'], result2['LTP_y'])
        sectors['LTP']= result3.result()
        table_df['LTP']= result4.result()
        executor.submit(sector_ltp)

def on_connect(ws, response):
    global sectors
    ws.subscribe(sectors['instrument_token'].to_list())
    ws.set_mode(ws.MODE_LTP,sectors['instrument_token'].to_list())

def is_today_holiday(date):
    try:
        url = f"https://api.upstox.com/v2/market/holidays/{date}"
        payload={}
        headers = {
        'Accept': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        response=response.text
        data = json.loads(response)
        for h in data['data'][0]['closed_exchanges']:
            if h=='NSE':
                return False
        return True
    except:
        return True

def download_data(kite, token):
    global api_call
    try:
        if (datetime.now()-api_call)>timedelta(seconds=0.333333):
            day_data=pd.DataFrame(kite.historical_data(token, datetime.now()-timedelta(days=2000),datetime.now() , 'day', continuous=False, oi=False))
            api_call=datetime.now()
            if len(day_data['close'])>252:
                day_data.set_index('date', inplace=True, drop=True)
                day_data.index = pd.to_datetime(day_data.index).date
                day_data=day_data.loc[~day_data.index.duplicated()]
                day_data.index.name = 'date'
                day_data.reset_index(drop=False, inplace=True)
                day_data.to_feather(f"Data/Day/{token}.feather")
            else:
                return False
        else:
            sleep(0.01)
            return download_data(kite, token)
    except Exception as e:
        print(token,e)

tokens_in=False
websocket_ltp=False
day_df=False
api_call=datetime.now()

while True:
    if previous_day.date()!=datetime.now().date() and (datetime.today().weekday() != 5) and (datetime.today().weekday() != 6) and is_today_holiday(datetime.now().date()):
        if (datetime.strptime('08:31', "%H:%M").time()<datetime.now().time()) and tokens_in==False:
        # if True:
            tokens_in=True
            kite= kt.KiteApp(config.name,config.client_code,config.token)
            sectors=pd.read_csv("scrap.csv")
            table_df=pd.DataFrame(columns=['Sector','NSE_Symbol','instrument_token'])
            inst=pd.DataFrame(kite.instruments(exchange='NSE'))
            inst_sector=inst[inst['tradingsymbol'].isin(sectors['NSE_Symbol'])]['tradingsymbol']
            no_symb=sectors[~sectors['NSE_Symbol'].isin(inst_sector)]['NSE_Symbol']
            for symbol in no_symb:
                try:
                    new_symb=inst[inst['tradingsymbol'].isin(stock_series(symbol))]['tradingsymbol'].iloc[0]
                    sectors['NSE_Symbol']=sectors['NSE_Symbol'].replace({symbol: new_symb})
                except:
                    sectors.drop(sectors[sectors['NSE_Symbol']==symbol].index,inplace=True)
            new_data=pd.DataFrame(columns=['Sector', 'NSE_Symbol','Comapny_Name'])
            for i in inst[inst['segment']=='INDICES']['instrument_token']:
                new_data.loc[i]={'Sector':'Nifty_Indices', 'NSE_Symbol':inst[inst['instrument_token']==i]['tradingsymbol'].iloc[0],'Comapny_Name':inst[inst['instrument_token']==i]['tradingsymbol'].iloc[0]}
            sectors = pd.concat([new_data, sectors], ignore_index=True)
            sectors['instrument_token'] = pd.merge(sectors, inst[inst['tradingsymbol'].isin(sectors['NSE_Symbol'])][['instrument_token','tradingsymbol']], left_on='NSE_Symbol',right_on='tradingsymbol', how='left')['instrument_token']
            for token in sectors['instrument_token']:
                if download_data(kite, token)==False:
                    sectors.drop(sectors[sectors['instrument_token']==token].index,inplace=True)
            for sector in sectors['Sector'].unique():
                if sectors[sectors['Sector']==sector]['NSE_Symbol'].count()==1:
                    os.remove(f"Data/Day/{sectors[sectors['Sector']==sector]['instrument_token'].iloc[0]}.feather")
                    sectors.drop(sectors[sectors['Sector']==sector].index,inplace=True)               
                else:
                    if sector!='Nifty_Indices':
                        table_df.loc[len(table_df['Sector'])]={'Sector':'Sector_Indices', 'NSE_Symbol':sector, 'instrument_token':sector}
            for sec in sectors['Sector'].unique():
                if sec!='Nifty_Indices':
                    symb_data=[]
                    for symb_token in sectors[sectors['Sector']==sec]['instrument_token']:
                        data=pd.read_feather(f'Data/Day/{symb_token}.feather')
                        data.set_index('date', inplace=True, drop=True)
                        data.index = pd.to_datetime(data.index).date
                        data.index.name='date'
                        data.rename(columns={'open':f'open{symb_token}','high':f'high{symb_token}','low':f'low{symb_token}','close':f'close{symb_token}','volume':f'volume{symb_token}'},inplace=True)
                        symb_data.insert(len(symb_data),data)
                    close = calculate_sector_df(pd.concat([col.iloc[:, 3] for col in symb_data], axis=1).dropna(),len(symb_data),'close')
                    open = calculate_sector_df(pd.concat([col.iloc[:, 0] for col in symb_data], axis=1).dropna(),len(symb_data),'open')
                    low = calculate_sector_df(pd.concat([col.iloc[:, 2] for col in symb_data], axis=1).dropna(),len(symb_data),'low')
                    high = calculate_sector_df(pd.concat([col.iloc[:, 1] for col in symb_data], axis=1).dropna(),len(symb_data),'high')
                    volume = calculate_sector_volume(pd.concat([col.iloc[:, 4] for col in symb_data], axis=1).dropna(),len(symb_data))
                    sector_df=pd.concat([open,high,low,close,volume], axis=1)
                    sector_df.reset_index(drop=False, inplace=True)
                    sector_df.to_feather(f"Data/Day/{sec}.feather")
            table_df=pd.concat([sectors,table_df], ignore_index=True)
            table_df['LTP']=0
            sectors['LTP']=0
            def ltp_e(instrument,s_t):
                try:
                    global sectors,table_df
                    idf=pd.read_feather(f'Data/Day/{instrument}.feather')
                    iltp=idf['close'].iloc[-1]
                    if s_t=='s':
                        index=sectors[sectors['instrument_token'] == instrument].index[0]
                        sectors.at[index, 'LTP']=iltp
                    elif s_t=='t':
                        index=table_df[table_df['instrument_token'] == instrument].index[0]
                        table_df.at[index, 'LTP']=iltp
                except:
                    print(instrument,s_t)
            with concurrent.futures.ThreadPoolExecutor(max_workers=4000) as executor:
                futures1=[executor.submit(ltp_e,instrument,'s') for instrument in sectors['instrument_token']]
                futures2=[executor.submit(ltp_e,instrument,'t') for instrument in table_df['instrument_token']]
            executor.shutdown(wait=True)
        if (datetime.now().time()>=datetime.strptime("09:15", "%H:%M").time()) and websocket_ltp==False:
            websocket_ltp=True
            kws = kite.kws()
            kws.on_ticks = on_ticks
            kws.on_connect = on_connect
            kws.connect(threaded = True)
            while True:
                if datetime.now().time()>=datetime.strptime("15:30", "%H:%M").time():
                    kws.close()
                    break
                sleep(1)
        # if datetime.now().hour>=17 and day_df==False:
        # # if True:'
        #     day_df=True
        #     for token in sectors['instrument_token']:
                
        #         sleep(0.333334)
        #     for sec in sectors['Sector'].unique():
        #         if sec!='Nifty_Indices':
        #             symb_data=[]
        #             for symb_token in sectors[sectors['Sector']==sec]['instrument_token']:
        #                 data=pd.read_feather(f'Data/Day/{symb_token}.feather')
        #                 data.set_index('date', inplace=True, drop=True)
        #                 data.index = pd.to_datetime(data.index).date
        #                 data.index.name='date'
        #                 data.rename(columns={'open':f'open{symb_token}','high':f'high{symb_token}','low':f'low{symb_token}','close':f'close{symb_token}','volume':f'volume{symb_token}'},inplace=True)
        #                 symb_data.insert(len(symb_data),data)
        #             close = calculate_sector_df(pd.concat([col.iloc[:, 3] for col in symb_data], axis=1).dropna(),len(symb_data),'close')
        #             open = calculate_sector_df(pd.concat([col.iloc[:, 0] for col in symb_data], axis=1).dropna(),len(symb_data),'open')
        #             low = calculate_sector_df(pd.concat([col.iloc[:, 2] for col in symb_data], axis=1).dropna(),len(symb_data),'low')
        #             high = calculate_sector_df(pd.concat([col.iloc[:, 1] for col in symb_data], axis=1).dropna(),len(symb_data),'high')
        #             volume = calculate_sector_volume(pd.concat([col.iloc[:, 4] for col in symb_data], axis=1).dropna(),len(symb_data))
        #             sector_df=pd.concat([open,high,low,close,volume], axis=1)
        #             sector_df.reset_index(drop=False, inplace=True)
        #             sector_df.to_feather(f"Data/Day/{sec}.feather")
        if (tokens_in==True) and (websocket_ltp==True):
            tokens_in=False
            websocket_ltp=False
            # day_df=False
            previous_day=datetime.now()
        # break