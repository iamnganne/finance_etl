import yfinance as yf 
import pandas as pd 
import os 
from datetime import date, datetime 
import yfinance.shared
yfinance.shared._DEFAULT_USER_AGENT = "Mozilla/5.0"
def extract_data_yahoofinance(tickers,start = "2021-01-01", end = "2024-12-31",auto_adjust=True, threads=False):
    all_data = [] 
    data = yf.download(tickers=tickers, start = start, end=end, group_by= 'ticker') 
    if isinstance(data.columns, pd.MultiIndex):
        for ticker in tickers: 
            data_ticker = data[ticker].copy().reset_index()
            data_ticker["Ticker"] = ticker 
            all_data.append(data_ticker[['Date','Ticker','Open','High','Low','Close','Volume']])    
    final_df = pd.concat(all_data).sort_values(by =['Date','Ticker']) 
    yahoo_finance_data = f"data/raw/yahoo_finance_data.csv"   
    final_df.to_csv(yahoo_finance_data, index = False) 
    return final_df 
if __name__ == "__main__":
    extract_data_yahoofinance(tickers = ['BTC-USD', 'ETH-USD', 'AAPL', 'MSFT', 'GOOGL'],start = "2021-01-01", end = "2024-12-31")