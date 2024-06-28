import pandas as pd
import streamlit as st
from lightweight_charts.widgets import StreamlitChart
from datetime import datetime, timedelta
from time import sleep
import concurrent.futures
from time import sleep
from copy import deepcopy
import numpy as np
import asyncio

# sectors=pd.read_csv("scrap.csv")
table_df=pd.DataFrame(columns=['Sector','NSE_Symbol','instrument_token'])

col1,col2=st.columns(2,gap="medium")
sector1 = col1.empty()
sector2 = col2.empty()

table1=col1.empty()
table2=col2.empty()

select_chart=col1.empty()
# chart1=col1.empty()

with st.sidebar.expander("Settings"):
    # comparative_symbol = st.sidebar.selectbox("Comparative Symbol", sectors['NSE_Symbol'],index=4)
    comparative_symbol = st.sidebar.empty()
    # c_instrument=int(sectors[sectors['NSE_Symbol']==comparative_symbol]['instrument_token'].iloc[0])
    static_length = st.sidebar.number_input("Static Period", value=123)
    adaptive_reference_date = st.sidebar.date_input("Adaptive_RS", value=datetime.strptime('2023-09-15', '%Y-%m-%d').date())
    timeframe = st.sidebar.selectbox("Timeframe", ["1hour", "2hour", "3hour", "Day", "Weekly"], index = 3)

chart = StreamlitChart(width=700, height=300)
chart.legend(visible=True)

# @st.cache_data
def chart_srs_ars(base_data, adaptive_reference_date,comparative_data,static_length):
    adaptive_length = (base_data.index > adaptive_reference_date).sum()

    static_rs = base_data / base_data.shift(static_length) / (comparative_data / comparative_data.shift(static_length)) - 1
    adaptive_rs = base_data.iloc[-adaptive_length-1:] / base_data.shift(adaptive_length).iloc[-adaptive_length-1:] / (comparative_data.iloc[-adaptive_length-1:] / comparative_data.shift(adaptive_length).iloc[-adaptive_length-1:]) - 1

    return static_rs , adaptive_rs

def is_numeric(str):
    try:
        int(str)
        return True
    except ValueError:
        return False

# cache_data_dict={k: v for k, v in st.session_state.items()}
def data(timeframe,instrument):
    # global cache_data_dict
    # if instrument in cache_data_dict.keys():
    #     return cache_data_dict[instrument]
    if True:
        if timeframe == '1hour':
            # sectors= pd.read_csv(f'Data/1hour/{instrument}.csv',index_col='date')
            # sectors.index = sectors.index.map(lambda x: pd.to_datetime(x))
            # return sectors
            pass
        elif timeframe == 'Day':
            # if is_numeric(instrument):
            #     sectors= pd.read_csv(f'Data/Day/{instrument}.csv',parse_dates=['date'],index_col='date')
            #     sectors.index = sectors.index.map(lambda x: x.date)
            # else:
            #     sectors=pd.read_csv(f'Data/Day/{instrument}.csv',parse_dates=['Unnamed: 0'],index_col='Unnamed: 0')
            #     sectors.index = sectors.index.map(lambda x: pd.to_datetime(x).date())
            day_data=pd.read_feather(f'Data/Day/{instrument}.feather')
            day_data.set_index('date', inplace=True, drop=True)
            day_data.index = pd.to_datetime(day_data.index).date
            day_data.index.name='date'
            # cache_data_dict[instrument]=day_data
            return day_data

cache_data_dict={k: v for k, v in st.session_state.items()}
def price_cache(timeframe,token,length):
    global cache_data_dict
    if f'{token}_{length}' not in cache_data_dict.keys():
        cache_data_dict[f'{token}_{length}']=data(timeframe,token)['close'].iloc[-length]
    return cache_data_dict[f'{token}_{length}']
    # if f'{token}_{length}' not in cache.keys():
    #     cache[f'{token}_{length}']=data(timeframe,token)['close'].iloc[-length]
    # return cache[f'{token}_{length}']

# def srs_ars(df,comparative_data,adaptive_reference_date,static_length,comparitive_token):
#     adaptive_length = (comparative_data.index > adaptive_reference_date).sum()
#     # comparitive_LTP=df[df['instrument_token']==comparitive_token]['LTP'].iloc[0]
#     adaptive_ratio=df[df['instrument_token']==comparitive_token]['LTP'].iloc[0]/comparative_data.iloc[-adaptive_length]
#     static_ratio=df[df['instrument_token']==comparitive_token]['LTP'].iloc[0]/comparative_data.iloc[-static_length]
    
#     def parallel_calculations(i,a_s):
#         # base_data = data('Day',df['instrument_token'].iloc[i])['close']
#         if a_s=='ars':
#             # df.loc[i,'ARS']=(df['LTP'].iloc[i]/ base_data.iloc[-adaptive_length] / (adaptive_ratio) - 1)
#             df.loc[i,'ARS']=(df['LTP'].iloc[i]/ price_cache('Day',df['instrument_token'].iloc[i],adaptive_length) / (adaptive_ratio) - 1)
#         elif a_s=='srs':
#             # df.loc[i,'SRS']=(df['LTP'].iloc[i]/ base_data.iloc[-static_length] / (static_ratio) - 1)
#             df.loc[i,'SRS']=(df['LTP'].iloc[i]/ price_cache('Day',df['instrument_token'].iloc[i],static_length) / (static_ratio) - 1)
#     with concurrent.futures.ThreadPoolExecutor(max_workers=4000)as executor:
#         futuresars=[executor.submit(parallel_calculations,i,'ars') for i in df.index]
#         futuressrs=[executor.submit(parallel_calculations,i,'srs') for i in df.index]
#     executor.shutdown(wait=True)
    
#     return df['SRS'].round(2),df['ARS'].round(2)
async def srs_ars(df, comparative_data, adaptive_reference_date, static_length, comparitive_token):
    adaptive_length = (comparative_data.index > adaptive_reference_date).sum()
    adaptive_ratio=df[df['instrument_token']==comparitive_token]['LTP'].iloc[0]/comparative_data.iloc[-adaptive_length]
    static_ratio=df[df['instrument_token']==comparitive_token]['LTP'].iloc[0]/comparative_data.iloc[-static_length]


    async def parallel_calculations(i, a_s):
        if a_s=='ars':
            # df.loc[i,'ARS']=(df['LTP'].iloc[i]/ base_data.iloc[-adaptive_length] / (adaptive_ratio) - 1)
            df.loc[i,'ARS']=(df['LTP'].iloc[i]/ price_cache('Day',df['instrument_token'].iloc[i],adaptive_length) / (adaptive_ratio) - 1)
        elif a_s=='srs':
            # df.loc[i,'SRS']=(df['LTP'].iloc[i]/ base_data.iloc[-static_length] / (static_ratio) - 1)
            df.loc[i,'SRS']=(df['LTP'].iloc[i]/ price_cache('Day',df['instrument_token'].iloc[i],static_length) / (static_ratio) - 1)
    try:
        futuresars = [asyncio.ensure_future(parallel_calculations(i, 'ars')) for i in df.index]
        futuressrs = [asyncio.ensure_future(parallel_calculations(i, 'srs')) for i in df.index]
        await asyncio.gather(*futuresars)
        await asyncio.gather(*futuressrs)

    except Exception as e:
        print(e)

    return df['SRS'].round(2), df['ARS'].round(2)

table_df=pd.read_csv("table_df.csv")

def dual_rank(table_df):
    # global table_df
    def rank_func(sectordf):
        # global table_df
        try:
            sorted_ars = sectordf['ARS'].rank(ascending=False,method='first')
            sorted_srs = sectordf['SRS'].rank(ascending=False,method='first')
            Dual_Rank = (sorted_ars + sorted_srs).rank(ascending=True,method='first')
            # result = pd.merge(table_df[['instrument_token','Dual_Rank']],sectordf[['instrument_token','Dual_Rank']], on='instrument_token', how='left')
            # table_df['Dual_Rank']= np.where(result['Dual_Rank_x'].isna(), result['Dual_Rank_y'], result['Dual_Rank_x'])
            return Dual_Rank
        except Exception as e:
            print(e)
    with concurrent.futures.ThreadPoolExecutor(max_workers=200)as executor:
        futures=[executor.submit(rank_func,table_df[table_df['Sector']==i]) for i in table_df['Sector'].unique()]
    executor.shutdown(wait=True)
    return pd.concat([results.result() for results in futures], axis=0)

comparative_symbol=comparative_symbol.selectbox("Comparative Symbol", table_df['NSE_Symbol'],index=3)
sector1=sector1.selectbox("Sector", table_df['Sector'].unique(), key="sector1")
sector2=sector2.selectbox("Sector", table_df['Sector'].unique(), key="sector2")

com_sym_token=table_df[table_df['NSE_Symbol']==comparative_symbol]['instrument_token'].iloc[0]

if 'ses_dict' not in st.session_state:
    st.session_state['ses_dict']=False

if 'table_cache' not in st.session_state:
    st.session_state['table_cache']=False
    st.session_state['table_df']=table_df

if 'today_date' not in st.session_state:
    st.session_state['today_date']=datetime.now().date()

base_symbol = select_chart.selectbox("Select a stock:", table_df['NSE_Symbol'])
bs_token =  table_df[table_df['NSE_Symbol']==base_symbol]['instrument_token'].iloc[0]

static_rs, adaptive_rs= chart_srs_ars(data('Day',bs_token)['close'], adaptive_reference_date,data('Day',com_sym_token)['close'],static_length)

Static_rs = static_rs.dropna().to_frame(name='Static_rs')
Adaptive_rs = adaptive_rs.dropna().to_frame(name='Adaptive_rs')

line1=chart.create_line(name='Static_rs',color='blue')
line2=chart.create_line(name='Adaptive_rs',color='purple')

line1.set(Static_rs)
line2.set(Adaptive_rs)

zero=pd.DataFrame([{"time": d.strftime('%Y-%m-%d'), "Zero": 0} for d in Static_rs.index])
zero_line=chart.create_line(name='Zero',color='#ff5252')
zero_line.set(zero)

chart.load()
print('rerun')
while True:
    print('iter')
    while (datetime.now().time()>=datetime.strptime("09:10", "%H:%M").time()) and (datetime.now().time()<=datetime.strptime("15:30", "%H:%M").time()):
    # if True:  
        try:
            time1=datetime.now()
            table_df=pd.read_csv("table_df.csv")
            print('srs')
            table_df['SRS'],table_df['ARS']=asyncio.run(srs_ars(table_df,data('Day',com_sym_token)['close'],adaptive_reference_date,static_length,com_sym_token))
            table_df['Dual_Rank']=dual_rank(table_df)
            table1.dataframe(table_df[table_df['Sector']==sector1][['Sector','NSE_Symbol','LTP','SRS','ARS','Dual_Rank']],use_container_width=True,hide_index=True)
            table2.dataframe(table_df[table_df['Sector']==sector2][['Sector','NSE_Symbol','LTP','SRS','ARS','Dual_Rank']],use_container_width=True,hide_index=True)
            print(datetime.now()-time1)
        except Exception as e:
            print(e)  
        st.session_state['table_cache']=True 
        if st.session_state['ses_dict']==False:
            st.session_state['ses_dict']=True
            for data_key in cache_data_dict.keys():
                try:
                    st.session_state[data_key]=cache_data_dict[data_key]
                except Exception as e:
                    print(e)
    if st.session_state['table_cache']==True:
        st.session_state['table_cache']=False
        st.session_state['table_df']=table_df
    print('table_cache')
    table1.dataframe(st.session_state['table_df'][st.session_state['table_df']['Sector']==sector1][['Sector','NSE_Symbol','LTP','SRS','ARS','Dual_Rank']],use_container_width=True,hide_index=True)
    table2.dataframe(st.session_state['table_df'][st.session_state['table_df']['Sector']==sector2][['Sector','NSE_Symbol','LTP','SRS','ARS','Dual_Rank']],use_container_width=True,hide_index=True)
        
        
    if st.session_state['today_date']!=datetime.now().date():
        if (datetime.now().hour>=9):
            cache_data_dict={}
            for key in st.session_state.keys():
                del st.session_state[key]
            st.session_state['ses_dict']=False
            st.session_state['today_date']=datetime.now().date()
    # break
    sleep(1)


# def calculate_sector_df(df,no_of_s,st):
#     sd=0
#     df[st]=0
#     for l in range(-no_of_s,0):
#         df[st]+=df.iloc[:, l-1]*abs(l)
#         sd+=abs(l)
#     df[st]=round((df[st]/sd),2)
#     return df[st]

# def calculate_sector_volume(df,no_of_s):
#     df['volume']=0
#     for l in range(-no_of_s,0):
#         df['volume']+=df.iloc[:, l-1]
#     return df['volume']

# def unique_col(lst):
#     unique_list = []
#     for item in lst:
#         if item.empty or item not in unique_list:
#             unique_list.append(item)
#     return unique_list

# def stock_series(stock):
#     return [f'{stock}-BE',f'{stock}-BZ']