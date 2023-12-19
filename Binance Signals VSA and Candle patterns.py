from binance.spot import Spot
from configparser import ConfigParser
import pandas as pd
import pandas_ta as ta
import telebot
import csv
from apscheduler.schedulers.background import BlockingScheduler
from pytz import utc

scheduler = BlockingScheduler(timezone = utc)

config = ConfigParser()
config.read('venv/config.ini')

bot = telebot.TeleBot(config['bot']['token'])
client = Spot(api_key=config['keys']['api_key'], api_secret=config['keys']['api_secret'])

pairs_list = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOGEUSDT', 'DOTUSDT', 'TRXUSDT',
              'LINKUSDT', 'MATICUSDT']

chat_id_lst = [469114030, 202194579]

#main logic
def crypto_spot_H(time_frame):
    for pair in pairs_list:
        df = pd.DataFrame(client.klines(str(pair), time_frame, limit=50), columns=['Date',
                                                                              'Open price', 'High price', 'Low price',
                                                                              'Close price', 'Volume', 'Kline Close time',
                                                                              'Quote asset volume',
                                                                              'Number of trades',
                                                                              'Taker buy base asset volume',
                                                                              'Taker buy quote asset volume',
                                                                              'Unused field'])

        df['Date'] = pd.to_datetime(df['Date'], unit='ms')

        df = df.drop(columns=['Kline Close time', 'Quote asset volume', 'Number of trades',
                              'Taker buy base asset volume','Taker buy quote asset volume','Unused field'])
        df[['Open price', 'High price', 'Low price', 'Close price', 'Volume']] = df[['Open price', 'High price',
                                                                                     'Low price', 'Close price',
                                                                                     'Volume']].astype(float)
        #VO
        df['Volume'] = df['Volume'].astype(int)
        df['EMA_5'] = df['Volume'].ewm(span=5, adjust=False).mean()
        df['EMA_10'] = df['Volume'].ewm(span=10, adjust=False).mean()
        df['VO'] = (df['EMA_5'] - df['EMA_10']) / df['EMA_10'] * 100

        #PPO
        df['Price_EMA_10'] = df['Close price'].ewm(span=10, adjust=False).mean()
        df['Price_EMA_21'] = df['Close price'].ewm(span=21, adjust=False).mean()
        df['PPO'] = (df['Price_EMA_10'] - df['Price_EMA_21']) / df['Price_EMA_21'] * 100

        cdl_df = df.ta.cdl_pattern(
            name=['xsidegap3methods', 'upsidegap2crows', 'eveningstar', 'morningstar', 'morningdojistar',
                  'eveningdojistar', 'gravestonedoji', 'dragonflydoji', 'hammer', 'invertedhammer', '3blackcrows',
                  '3whitesoldiers', 'marubozu', 'risefall3methods', 'mathold'])
        # info to send
        time = df['Date'].iloc[-2]
        searching_row = cdl_df.iloc[-2,:]
        pattern = searching_row[searching_row != 0].to_string()
        vo = df['VO'].iloc[-2].round(2)
        prcnt_change = (df.iloc[-2]['Close price'] - df.iloc[-2]['Open price']) / df.iloc[-2]['Close price'] * 100
        flat_value = df['PPO'][-20:].loc[(df['PPO'] < 0.4 ) & (df['PPO'] > -0.4)].mean()

        if searching_row[searching_row != 0].to_numpy().size != 0:
            with open('logs.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([pair, time_frame, time, pattern, 'Binance', vo])
            print(f'{pair} {time_frame} pattern is found')
            for chat in chat_id_lst:
                bot.send_message(chat_id=chat, text= f'{pair}\n{time_frame}\n{time}\npattern: {pattern}\nVO: {vo}%')

        elif vo > 20:
            print(f'{pair} {time_frame} INCREASED VOLUME is found')
            with open('logs.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([pair, time_frame, time, f'INCREASED VOLUME {prcnt_change}%', 'Binance', vo])
            for chat in chat_id_lst:
                bot.send_message(chat_id=chat, text=f'{pair}\n{time_frame}\n{time}\npattern: INCREASED VOLUME\n'
                                                    f'% change: {prcnt_change}\nVO: {vo}%')

        elif df['PPO'][-12:].loc[(df['PPO'] < 0.4 ) & (df['PPO'] > -0.4)].size == 12:
            print(f'{pair} {time_frame} FLAT_12 is found')
            with open('logs.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([pair, time_frame, time, f'FLAT_12 {flat_value}', 'Binance', vo])
            for chat in chat_id_lst:
                bot.send_message(chat_id=chat, text=f'{pair}\n{time_frame}\n{time}\npattern: FLAT_12\n'
                                                    f'mean value: {flat_value}\nVO: {vo}%')

        else:
            print(f'{pair} {time_frame} nothing found')
            pass

    print(f'Finish {time_frame}')

job_h = scheduler.add_job(crypto_spot_H, 'cron', hour = '*', minute = 0, second = 10, args=['1h'])
job_4h = scheduler.add_job(crypto_spot_H, 'cron', hour = '*/4', minute = 0, second = 20, args=['4h'])
job_1d = scheduler.add_job(crypto_spot_H, 'cron', day = '*', hour = 0, minute = 1, second = 20, args=['1d'])
scheduler.start()