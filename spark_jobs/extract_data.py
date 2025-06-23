from pycoingecko import CoinGeckoAPI
import pandas as pd 
import requests 
import time
from datetime import date,datetime, timedelta
def extract_data_coin(start_time = "2018-01-01", end_time = "2024-12-31", step_days = 100):
    cg = CoinGeckoAPI()
    #coin_ids = ["bitcoin","etherum","binancecoin","solana","cardano"]
    start_day = datetime.strptime(start_time,"%Y-%m-%d")
    end_day = datetime.strptime(end_time,"%Y-%m-%d")
    while start_day < end_day: 
        from_timestamp = int(start_day.timestamp())
        to_timestamp = min(from_timestamp + timedelta(days=step_days),end_day)
        to_timestamp = int(to_timestamp.timestamp())
    data = cg.get_coin_market_chart_range_by_id(id="bitcoin",vs_currency='usd', from_timestamp = from_timestamp, to_timestamp = to_timestamp)
    # Get current price of Bitcoin in USD
    df_prices = pd.DataFrame(data['prices'], columns=['timestamp','price'])
    df_marketcap = pd.DataFrame(data['market_caps'], columns=['timestamp','market_cap'])
    df_totalvolume = pd.DataFrame(data['total_volumes'], columns=['timestamp','volume'])
    #change timestamp to date
    df_prices['date'] = pd.to_datetime(df_prices['timestamp'], unit = 'ms')
    df_marketcap['date'] = pd.to_datetime(df_marketcap['timestamp'], unit = 'ms')
    df_totalvolume['date'] = pd.to_datetime(df_totalvolume['timestamp'], unit = 'ms')
    
    df = df_prices[['date','price']].merge(df_marketcap[['date','market_cap']], on = 'date').merge(df_totalvolume[['date','volume']], on ='date')

    #data.to_csv("crypto_market_data1.csv", index = False)
    df = df.sort_values('date')
    crypto_market_data = f"data/raw/crypto_market_data.csv"   
    df.to_csv(crypto_market_data, index = False)
    return df
    #print(df.head(10))

if __name__ == "__main__":
    extract_data_coin(start_time = "2018-01-01", end_time = "2024-12-31", step_days = 100)

    
