import os
import sys
node_path = "/home/doku/.nvm/versions/node/v20.11.0/bin/node"
script_directory = os.path.dirname(__file__)
parent_directory = os.path.dirname(script_directory)
parent_parent_directory = os.path.dirname(parent_directory)
sys.path.append(parent_parent_directory)
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import matplotlib.pyplot as plt
import ta
import numpy as np
import datetime
import subprocess
import sqlite3
import glob
import shutil
from IPython.display import clear_output
from utilities.data_manager import ExchangeDataManager
from utilities.custom_indicators import get_n_columns, SuperTrend
from utilities.backtesting import basic_single_asset_backtest, plot_wallet_vs_asset, get_metrics, get_n_columns, plot_sharpe_evolution, plot_bar_by_month
from utilities.custom_indicators import get_n_columns
import traceback

try:

    class SaEnvelope():
        def __init__(
            self,
            df,
            df_btc,
            type=["long"],
            ma_base_window=3,
            envelopes=[0.05, 0.1, 0.15],
            source_name="close",
            coef_on_btc_rsi=1.6,       
            coef_on_stoch_rsi=1.3,
        ):
            self.df = df
            self.df_btc = df_btc
            self.use_long = True if "long" in type else False
            self.use_short = True if "short" in type else False
            self.ma_base_window = ma_base_window
            self.envelopes = envelopes
            self.source_name = source_name        
            self.coef_on_btc_rsi = coef_on_btc_rsi
            self.coef_on_stoch_rsi = coef_on_stoch_rsi

        def get_source(self, df, source_name):
            if source_name == "close":
                return df["close"]
            elif source_name == "hl2":
                return (df['high'] + df['low']) / 2
            elif source_name == "hlc3":
                return (df['high'] + df['low'] + df['close']) / 3
            elif source_name == "ohlc4":
                return (df['open'] + df['high'] + df['low'] + df['close']) / 4
            elif source_name == "hlcc4":
                return (df['high'] + df['low'] + df['close'] + df['close']) / 4   
            
        def populate_indicators(self):
            # -- Clear dataset --
            df = self.df
            df.drop(
                columns=df.columns.difference(['open','high','low','close','volume']), 
                inplace=True
            )

            df_btc = self.df_btc
            # -- Populate indicators --            
            df_btc['rsi'] = ta.momentum.RSIIndicator(close=df_btc['close'], window=14).rsi().shift(1)
            df_btc['coef_low_env'] = 1.0
            df_btc.loc[(df_btc['rsi'] <= 30), 'coef_low_env'] = self.coef_on_btc_rsi  
            df_btc['coef_high_env'] = 1.0
            df_btc.loc[(df_btc['rsi'] >= 70), 'coef_high_env'] = self.coef_on_btc_rsi
            df_btc_coef_env = df_btc[['coef_low_env', 'coef_high_env']].copy()
            df = df.merge(df_btc_coef_env,how='left', left_on='date', right_on='date')

            df['k'] = ta.momentum.StochRSIIndicator(df['close']).stochrsi_k().shift(1)
            df.loc[(df['k'] >= 0.75), 'coef_low_env'] = self.coef_on_stoch_rsi

            src = self.get_source(df, self.source_name) 
            df['ma_base'] = ta.trend.sma_indicator(close=src, window=self.ma_base_window).shift(1)
            high_envelopes = [round(1/(1-e)-1, 3) for e in self.envelopes]
            # low_envelopes = [round(abs(1/(1+e)-1), 3) for e in self.envelopes]
            for i in range(1, len(self.envelopes) + 1):
                df[f'ma_high_{i}'] = df['ma_base'] * (1 + df['coef_high_env']*high_envelopes[i-1])
                df[f'ma_low_{i}'] = df['ma_base'] * (1 - df['coef_low_env']*self.envelopes[i-1])
            
            self.df = df    
            return self.df
        
        def populate_buy_sell(self): 
            df = self.df
            # -- Initiate populate --
            df["close_long"] = False
            df["close_short"] = False

            for i in range(1, len(self.envelopes) + 1):
                df[f"open_short_{i}"] = False
                df[f"open_long_{i}"] = False
            
            if self.use_long:
                # -- Populate open long--
                for i in range(1, len(self.envelopes) + 1):
                    df.loc[
                        (df['low'] <= df[f'ma_low_{i}'])
                        , f"open_long_{i}"
                    ] = True
                
                # -- Populate close long limit --
                df.loc[
                    (df['high'] >= df['ma_base'])
                    , "close_long"
                ] = True
                
            
            if self.use_short:
                # -- Populate open short limit --
                for i in range(1, len(self.envelopes) + 1):
                    df.loc[
                        (df['high'] >= df[f'ma_high_{i}'])
                        , f"open_short_{i}"
                    ] = True
                
                # -- Populate close short market --
                df.loc[
                    (df['low'] <= df['ma_base'])
                    , "close_short"
                ] = True
            
            self.df = df   
            return self.df
            
        def run_backtest(self, initial_wallet=1000, leverage=1):
            df = self.df[:]
            wallet = initial_wallet
            maker_fee = 0.0008
            taker_fee = 0.0008
            trades = []
            days = []
            current_day = 0
            previous_day = 0
            current_position = None

            for index, row in df.iterrows():
                
                # -- Add daily report --
                current_day = index.day
                if previous_day != current_day:
                    temp_wallet = wallet
                    if current_position:
                        if current_position['side'] == "LONG":
                            close_price = row['close']
                            trade_result = (close_price - current_position['price']) / current_position['price']
                            temp_wallet += current_position['size'] * trade_result
                            fee = temp_wallet * taker_fee
                            temp_wallet -= fee
                        elif current_position['side'] == "SHORT":
                            close_price = row['close']
                            trade_result = (current_position['price'] - close_price) / current_position['price']
                            temp_wallet += current_position['size'] * trade_result
                            fee = temp_wallet * taker_fee
                            temp_wallet -= fee
                        
                    days.append({
                        "day":str(index.year)+"-"+str(index.month)+"-"+str(index.day),
                        "wallet":temp_wallet,
                        "price":row['close']
                    })
                previous_day = current_day
                if current_position:
                # -- Check for closing position --
                    if current_position['side'] == "LONG":                     
                        # -- Close LONG market --
                        if row['close_long'] and (current_position["envelope"] == len(self.envelopes) or row[f'open_long_{current_position["envelope"] + 1}'] is False):
                            close_price = row['ma_base']
                            trade_result = (close_price - current_position['price']) / current_position['price']
                            wallet += current_position['size'] * trade_result
                            close_trade_size = current_position['size'] + (current_position['size'] * trade_result)
                            fee = close_trade_size * maker_fee
                            wallet -= fee
                            trades.append({
                                "open_date": current_position['date'],
                                "close_date": index,
                                "position": "LONG",
                                "open_reason": current_position['reason'],
                                "close_reason": "Limit",
                                "open_price": current_position['price'],
                                "close_price": close_price,
                                "open_fee": current_position['fee'],
                                "close_fee": fee,
                                "open_trade_size":current_position['size'],
                                "close_trade_size": close_trade_size,
                                "wallet": wallet
                            })
                            current_position = None
                            
                    elif current_position['side'] == "SHORT":
                        # -- Close SHORT Limit --
                        if row['close_short'] and (current_position["envelope"] == len(self.envelopes) or row[f'open_short_{current_position["envelope"] + 1}'] is False):
                            # if current_position['reason'] == "Limit Envelop 3":
                            #     print("ok")
                            close_price = row['ma_base']
                            trade_result = (current_position['price'] - close_price) / current_position['price']
                            wallet += current_position['size'] * trade_result
                            close_trade_size = current_position['size'] + (current_position['size'] * trade_result)
                            fee = close_trade_size * maker_fee
                            wallet -= fee
                            trades.append({
                                "open_date": current_position['date'],
                                "close_date": index,
                                "position": "SHORT",
                                "open_reason": current_position['reason'],
                                "close_reason": "Limit",
                                "open_price": current_position['price'],
                                "close_price": close_price,
                                "open_fee": current_position['fee'],
                                "close_fee": fee,
                                "open_trade_size": current_position['size'],
                                "close_trade_size": close_trade_size,
                                "wallet": wallet
                            })
                            current_position = None

                # -- Check for opening position --
                for i in range(1, len(self.envelopes) + 1):
                    # -- Open long Limit --
                    if row[f'open_long_{i}']:
                        if current_position and (current_position["envelope"] >= i or current_position["side"] == "SHORT"):
                            continue
                        open_price = row[f'ma_low_{i}']
                        fee = wallet * maker_fee * (1/len(self.envelopes)) * leverage
                        wallet -= fee
                        pos_size = wallet * (1/len(self.envelopes)) * leverage
                        if current_position:
                            current_position["price"] = (current_position["size"] * current_position["price"] + open_price * pos_size) / (current_position["size"] + pos_size)
                            current_position["size"] = current_position["size"] + pos_size
                            current_position["fee"] = current_position["fee"] + fee
                            current_position["envelope"] = i
                            current_position["reason"] = f"Limit Envelop {i}"
                        else:
                            current_position = {
                                "size": pos_size,
                                "date": index,
                                "price": open_price,
                                "fee":fee,
                                "reason": f"Limit Envelop {i}",
                                "side": "LONG",
                                "envelope": i,
                            }
                    elif row[f'open_short_{i}']:
                        if current_position and (current_position["envelope"] >= i or current_position["side"] == "LONG"):
                            continue
                        open_price = row[f'ma_high_{i}']
                        fee = wallet * maker_fee * (1/len(self.envelopes)) * leverage
                        wallet -= fee
                        pos_size = wallet * (1/len(self.envelopes)) * leverage
                        if current_position:
                            current_position["price"] = (current_position["size"] * current_position["price"] + open_price * pos_size) / (current_position["size"] + pos_size)
                            current_position["size"] = current_position["size"] + pos_size
                            current_position["fee"] = current_position["fee"] + fee
                            current_position["envelope"] = i
                            current_position["reason"] = f"Limit Envelop {i}"
                        else:
                            current_position = {
                                "size": pos_size,
                                "date": index,
                                "price": open_price,
                                "fee":fee,
                                "reason": f"Limit Envelop {i}",
                                "side": "SHORT",
                                "envelope": i,
                            }
                    else:
                        break
                        
                        
            df_days = pd.DataFrame(days)
            df_days['day'] = pd.to_datetime(df_days['day'])
            df_days = df_days.set_index(df_days['day'])

            df_trades = pd.DataFrame(trades)
            if df_trades.empty:
                print("!!! No trades")
                return None
            else:
                df_trades['open_date'] = pd.to_datetime(df_trades['open_date'])
                df_trades = df_trades.set_index(df_trades['open_date'])  
            
            return get_metrics(df_trades, df_days) | {
                "wallet": wallet,
                "trades": df_trades,
                "days": df_days
            }      
            
    exchange_name = "bitget"
    database_directory = os.path.join(parent_parent_directory, 'database')
    bitget = ExchangeDataManager(
        exchange_name=exchange_name, 
        path_download=database_directory
    )
    markets = bitget.exchange.load_markets()

    files_path = os.path.join(database_directory, 'bitget', '30m', '*')
    files = glob.glob(files_path)
    for f in files:
        os.remove(f)
    download_data_path_script = os.path.join(database_directory, 'download_data.js')
    for market in markets:
        if 'USDT' in market and markets[market]['spot'] == True:        
            print(market)
            p = subprocess.Popen([node_path, download_data_path_script, f'{market}'], env=os.environ, stdout=subprocess.PIPE)
            out = p.stdout.read()
            print(out)    

    print('>>> create week_backtesting.db3')
    week_db_name =  os.path.join(database_directory, 'week_backtesting.db3')
    os.remove(week_db_name) if os.path.exists(week_db_name) else None
    tf = "30m"

    list_source_name = ['close', 'hl2', 'hlc3', 'ohlc4', 'hlcc4']
    list_env_perc = [0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09, 0.095, 0.1]

    df_btc = bitget.load_data(coin="BTC/USDT", interval=tf)
    coef_on_btc_rsi = 1.6
    coef_on_stoch_rsi = 1.3
    for market in markets:
        if 'USDT' in market and markets[market]['spot'] == True:
            print(market)        
            try:
                df = bitget.load_data(coin=market, interval=tf)
                df['volume_usdt'] = df['volume'] * df['close']
                last_volume_usdt = round(df['volume_usdt'].rolling(window=48).mean().iloc[-1], 2)
                if (last_volume_usdt >= 1000):
                    for i in range(0, len(list_source_name)) :
                        for j in range(0, len(list_env_perc)) :                        
                            strat = SaEnvelope( 
                                df = df.loc[:],
                                df_btc = df_btc,
                                type=["long"],
                                ma_base_window=5,
                                envelopes=[list_env_perc[j]],
                                source_name=list_source_name[i],
                                coef_on_btc_rsi=coef_on_btc_rsi,
                                coef_on_stoch_rsi=coef_on_stoch_rsi            
                            )
                            strat.populate_indicators()
                            strat.populate_buy_sell()
                            bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                            
                            if bt_result is not None:
                                df_trades, df_days = basic_single_asset_backtest(db_name=week_db_name,pair=market, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=list_env_perc[j], source_name=list_source_name[i], coef_on_btc_rsi=coef_on_btc_rsi, coef_on_stoch_rsi=coef_on_stoch_rsi)                        
            except FileNotFoundError:
                print(f'FileNotFoundError for {market}')

    print('>>> create profit_week_backtesting.db3')
    profit_week_db_name =  os.path.join(database_directory, 'profit_week_backtesting.db3')
    os.remove(profit_week_db_name) if os.path.exists(profit_week_db_name) else None
    shutil.copyfile(week_db_name, profit_week_db_name)

    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
    con.commit()
    cur.close()
    con.close()

    print('>>> optimize env_perc for profit_week_backtesting.db3')
    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    cur.execute(f"SELECT pair, source_name, env_perc, last_volume_usdt FROM backtesting")
    rows = cur.fetchall()
    rows_list = [list(row) for row in rows]
    cur.close()
    con.close() 

    for row in rows_list:
        pair = row[0]
        source_name = row[1]
        env_perc = row[2]
        last_volume_usdt = row[3]
        print(pair)
        df = bitget.load_data(coin=pair, interval=tf)

        # test env_perc    
        start_env = round(env_perc - 0.004, 5) #python gros pb d'arrondi avec les float....    
        end_env = round(env_perc + 0.004, 5)
        
        for i in np.linspace(start_env, end_env, 9):
            env_perc_to_test = round(i, 5) #python gros pb d'arrondi avec les float....    
            if env_perc_to_test != env_perc :
                strat = SaEnvelope( 
                    df = df.loc[:],
                    df_btc = df_btc,
                    type=["long"],
                    ma_base_window=5,
                    envelopes=[i],
                    source_name=source_name,
                    coef_on_btc_rsi=coef_on_btc_rsi,
                    coef_on_stoch_rsi=coef_on_stoch_rsi           
                )
                strat.populate_indicators()
                strat.populate_buy_sell()
                bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                if bt_result is not None:
                    df_trades, df_days = basic_single_asset_backtest(db_name=profit_week_db_name,pair=pair, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=env_perc_to_test, source_name=source_name, coef_on_btc_rsi=coef_on_btc_rsi, coef_on_stoch_rsi=coef_on_stoch_rsi)          
            
    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
    con.commit()
    cur.close()
    con.close()         

    print('>>> optimize coef_on_btc_rsi for profit_week_backtesting.db3')
    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    cur.execute(f"SELECT pair, source_name, env_perc, last_volume_usdt, coef_on_btc_rsi FROM backtesting")
    rows = cur.fetchall()
    rows_list = [list(row) for row in rows]
    cur.close()
    con.close() 

    for row in rows_list:
        pair = row[0]
        source_name = row[1]
        env_perc = row[2]
        last_volume_usdt = row[3]
        coef_on_btc_rsi = row[4]
        print(pair)
        df = bitget.load_data(coin=pair, interval=tf)

        # test coef_on_btc_rsi    
        start_coef_on_btc_rsi = 1   
        end_coef_on_btc_rsi = 3
        
        for i in np.linspace(start_coef_on_btc_rsi, end_coef_on_btc_rsi, 21):
            coef_on_btc_rsi_to_test = round(i, 2) #python gros pb d'arrondi avec les float....    
            if coef_on_btc_rsi_to_test != coef_on_btc_rsi :
                strat = SaEnvelope( 
                    df = df.loc[:],
                    df_btc = df_btc,
                    type=["long"],
                    ma_base_window=5,
                    envelopes=[env_perc],
                    source_name=source_name,
                    coef_on_btc_rsi=coef_on_btc_rsi_to_test,
                    coef_on_stoch_rsi=coef_on_stoch_rsi           
                )
                strat.populate_indicators()
                strat.populate_buy_sell()
                bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                if bt_result is not None:
                    df_trades, df_days = basic_single_asset_backtest(db_name=profit_week_db_name,pair=pair, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=env_perc, source_name=source_name, coef_on_btc_rsi=coef_on_btc_rsi_to_test, coef_on_stoch_rsi=coef_on_stoch_rsi)          
            
    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND coef_on_btc_rsi != (SELECT MAX(coef_on_btc_rsi) FROM backtesting WHERE pair = '{market}')")
    con.commit()
    cur.close()
    con.close()          

    print('>>> optimize coef_on_stoch_rsi for profit_week_backtesting.db3')
    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    cur.execute(f"SELECT pair, source_name, env_perc, last_volume_usdt, coef_on_btc_rsi, coef_on_stoch_rsi FROM backtesting")
    rows = cur.fetchall()
    rows_list = [list(row) for row in rows]
    cur.close()
    con.close() 

    for row in rows_list:
        pair = row[0]
        source_name = row[1]
        env_perc = row[2]
        last_volume_usdt = row[3]
        coef_on_btc_rsi = row[4]
        coef_on_stoch_rsi = row[5]
        print(pair)
        df = bitget.load_data(coin=pair, interval=tf)

        # test coef_on_stoch_rsi    
        start_coef_on_stoch_rsi = 1   
        end_coef_on_stoch_rsi = 3
        
        for i in np.linspace(start_coef_on_stoch_rsi, end_coef_on_stoch_rsi, 21):
            coef_on_stoch_rsi_to_test = round(i, 2) #python gros pb d'arrondi avec les float....    
            if coef_on_stoch_rsi_to_test != coef_on_stoch_rsi :
                strat = SaEnvelope( 
                    df = df.loc[:],
                    df_btc = df_btc,
                    type=["long"],
                    ma_base_window=5,
                    envelopes=[env_perc],
                    source_name=source_name,
                    coef_on_btc_rsi=coef_on_btc_rsi,
                    coef_on_stoch_rsi=coef_on_stoch_rsi_to_test           
                )
                strat.populate_indicators()
                strat.populate_buy_sell()
                bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                if bt_result is not None:
                    df_trades, df_days = basic_single_asset_backtest(db_name=profit_week_db_name,pair=pair, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=env_perc, source_name=source_name, coef_on_btc_rsi=coef_on_btc_rsi, coef_on_stoch_rsi=coef_on_stoch_rsi_to_test)          
            
    con = sqlite3.connect(profit_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND coef_on_btc_rsi != (SELECT MAX(coef_on_btc_rsi) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND coef_on_stoch_rsi != (SELECT MAX(coef_on_stoch_rsi) FROM backtesting WHERE pair = '{market}')")            
    con.commit()
    cur.close()
    con.close()     

    print('>>> create winrate_week_backtesting.db3')
    winrate_week_db_name =  os.path.join(database_directory, 'winrate_week_backtesting.db3')
    os.remove(winrate_week_db_name) if os.path.exists(winrate_week_db_name) else None
    shutil.copyfile(week_db_name, winrate_week_db_name) 
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
        cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND win_rate != 100")
        cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND avg_profit != (SELECT MAX(avg_profit) FROM backtesting WHERE pair = '{market}')")
        cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
        cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")      
        cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")    
    con.commit()
    cur.close()
    con.close()  

    print('>>> optimize env_perc for winrate_week_backtesting.db3')
    coef_on_btc_rsi = 1.6
    coef_on_stoch_rsi = 1.3
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    cur.execute(f"SELECT pair, source_name, env_perc, last_volume_usdt FROM backtesting")
    rows = cur.fetchall()
    rows_list = [list(row) for row in rows]
    cur.close()
    con.close() 

    for row in rows_list:
        pair = row[0]
        source_name = row[1]
        env_perc = row[2]
        last_volume_usdt = row[3]
        print(pair)
        df = bitget.load_data(coin=pair, interval=tf)

        # test env_perc    
        start_env = round(env_perc - 0.004, 5) #python gros pb d'arrondi avec les float....    
        end_env = round(env_perc + 0.004, 5)
        
        for i in np.linspace(start_env, end_env, 9):
            env_perc_to_test = round(i, 5) #python gros pb d'arrondi avec les float....    
            if env_perc_to_test != env_perc :
                strat = SaEnvelope( 
                    df = df.loc[:],
                    df_btc = df_btc,
                    type=["long"],
                    ma_base_window=5,
                    envelopes=[i],
                    source_name=source_name,
                    coef_on_btc_rsi=coef_on_btc_rsi,
                    coef_on_stoch_rsi=coef_on_stoch_rsi           
                )
                strat.populate_indicators()
                strat.populate_buy_sell()
                bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                if bt_result is not None:
                    df_trades, df_days = basic_single_asset_backtest(db_name=winrate_week_db_name,pair=pair, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=env_perc_to_test, source_name=source_name, coef_on_btc_rsi=coef_on_btc_rsi, coef_on_stoch_rsi=coef_on_stoch_rsi)          
            
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND win_rate != 100")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND avg_profit != (SELECT MAX(avg_profit) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
    con.commit()
    cur.close()
    con.close()  

    print('>>> optimize coef_on_btc_rsi for winrate_week_backtesting.db3')
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    cur.execute(f"SELECT pair, source_name, env_perc, last_volume_usdt, coef_on_btc_rsi FROM backtesting")
    rows = cur.fetchall()
    rows_list = [list(row) for row in rows]
    cur.close()
    con.close() 

    for row in rows_list:
        pair = row[0]
        source_name = row[1]
        env_perc = row[2]
        last_volume_usdt = row[3]
        coef_on_btc_rsi = row[4]
        print(pair)
        df = bitget.load_data(coin=pair, interval=tf)

        # test coef_on_btc_rsi    
        start_coef_on_btc_rsi = 1   
        end_coef_on_btc_rsi = 3
        
        for i in np.linspace(start_coef_on_btc_rsi, end_coef_on_btc_rsi, 21):
            coef_on_btc_rsi_to_test = round(i, 2) #python gros pb d'arrondi avec les float....    
            if coef_on_btc_rsi_to_test != coef_on_btc_rsi :
                strat = SaEnvelope( 
                    df = df.loc[:],
                    df_btc = df_btc,
                    type=["long"],
                    ma_base_window=5,
                    envelopes=[env_perc],
                    source_name=source_name,
                    coef_on_btc_rsi=coef_on_btc_rsi_to_test,
                    coef_on_stoch_rsi=coef_on_stoch_rsi           
                )
                strat.populate_indicators()
                strat.populate_buy_sell()
                bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                if bt_result is not None:
                    df_trades, df_days = basic_single_asset_backtest(db_name=winrate_week_db_name,pair=pair, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=env_perc, source_name=source_name, coef_on_btc_rsi=coef_on_btc_rsi_to_test, coef_on_stoch_rsi=coef_on_stoch_rsi)          
            
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND win_rate != 100")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND avg_profit != (SELECT MAX(avg_profit) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND coef_on_btc_rsi != (SELECT MAX(coef_on_btc_rsi) FROM backtesting WHERE pair = '{market}')")
    con.commit()
    cur.close()
    con.close()  

    print('>>> optimize coef_on_stoch_rsi for winrate_week_backtesting.db3')
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    cur.execute(f"SELECT pair, source_name, env_perc, last_volume_usdt, coef_on_btc_rsi, coef_on_stoch_rsi FROM backtesting")
    rows = cur.fetchall()
    rows_list = [list(row) for row in rows]
    cur.close()
    con.close() 

    for row in rows_list:
        pair = row[0]
        source_name = row[1]
        env_perc = row[2]
        last_volume_usdt = row[3]
        coef_on_btc_rsi = row[4]
        coef_on_stoch_rsi = row[5]
        print(pair)
        df = bitget.load_data(coin=pair, interval=tf)

        # test coef_on_stoch_rsi    
        start_coef_on_stoch_rsi = 1   
        end_coef_on_stoch_rsi = 3
        
        for i in np.linspace(start_coef_on_stoch_rsi, end_coef_on_stoch_rsi, 21):
            coef_on_stoch_rsi_to_test = round(i, 2) #python gros pb d'arrondi avec les float....    
            if coef_on_stoch_rsi_to_test != coef_on_stoch_rsi :
                strat = SaEnvelope( 
                    df = df.loc[:],
                    df_btc = df_btc,
                    type=["long"],
                    ma_base_window=5,
                    envelopes=[env_perc],
                    source_name=source_name,
                    coef_on_btc_rsi=coef_on_btc_rsi,
                    coef_on_stoch_rsi=coef_on_stoch_rsi_to_test           
                )
                strat.populate_indicators()
                strat.populate_buy_sell()
                bt_result = strat.run_backtest(initial_wallet=100, leverage=1)
                if bt_result is not None:
                    df_trades, df_days = basic_single_asset_backtest(db_name=winrate_week_db_name,pair=pair, trades=bt_result['trades'], days=bt_result['days'], last_volume_usdt=last_volume_usdt, env_perc=env_perc, source_name=source_name, coef_on_btc_rsi=coef_on_btc_rsi, coef_on_stoch_rsi=coef_on_stoch_rsi_to_test)          
            
    con = sqlite3.connect(winrate_week_db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()  
    for market in markets:
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND win_rate != 100")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND avg_profit != (SELECT MAX(avg_profit) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND usd_per_day != (SELECT MAX(usd_per_day) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND sharpe_ratio != (SELECT MAX(sharpe_ratio) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND env_perc != (SELECT MAX(env_perc) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND coef_on_btc_rsi != (SELECT MAX(coef_on_btc_rsi) FROM backtesting WHERE pair = '{market}')")
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND coef_on_stoch_rsi != (SELECT MAX(coef_on_stoch_rsi) FROM backtesting WHERE pair = '{market}')")            
            cur.execute(f"DELETE FROM backtesting WHERE pair = '{market}' AND source_name != (SELECT MAX(source_name) FROM backtesting WHERE pair = '{market}')")            
    con.commit()
    cur.close()
    con.close()  

    shutil.copyfile(winrate_week_db_name, '/home/doku/envelope/database/week_backtesting100.db3')                       
except:
    print(traceback.format_exc())
