import yfinance as yf
import pandas as pd
import os
import requests
from io import StringIO

class PriceLoader:
    def __init__(self, start_date = '2020-01-01', end_date = '2025-01-01'):
        # fetch from wikipedia
        self.fetch_snp500_symbols()

        # read from csv
        snp500_symbols = pd.read_csv('sp500_symbols.csv')

        # download data
        snp500_data = self.batch_download_data(sp500_symbols = snp500_symbols, start_date = start_date, end_date = end_date)

        # save to parquet
        self.save_combined_dataframe_to_parquet(combined_df = snp500_data)

    def test(self):
        return 123

    def fetch_snp500_symbols(self):
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers)

        tables = pd.read_html(StringIO(response.text))
        sp500_df = tables[0]

        sp500_df['Symbol'].to_csv('sp500_symbols.csv', index=False)

    def batch_download_data(self, sp500_symbols, batch_size = 15, start_date = '2020-01-01', end_date = '2025-01-01'):

        all_ticker_data = []

        for i in range(0, len(sp500_symbols), batch_size):

            batch = sp500_symbols[i:i+batch_size]
            tickers = batch.iloc[:, 0].tolist()

            df = yf.download(tickers, start=start_date, end=end_date)

            if len(tickers) == 1:
                batch_data = {
                        'Close': df['Close'].to_frame(tickers[0]),
                        'Volume': df['Volume'].to_frame(tickers[0])
                    }
            else:   
                    # For multiple tickers, extract Close and Volume separately
                batch_data = {
                    'Close': df['Close'],
                    'Volume': df['Volume']
                }

            all_ticker_data.append(batch_data)
        
        combined_close = pd.concat([data['Close'] for data in all_ticker_data], axis=1)
        combined_volume = pd.concat([data['Volume'] for data in all_ticker_data], axis=1)
        
        return {'Close': combined_close, 'Volume': combined_volume}

    def save_combined_dataframe_to_parquet(self,combined_df, data_dir='data'):
    
        for ticker in combined_df['Close'].columns:
            try:
                ticker_data = pd.DataFrame({
                    'Date': combined_df['Close'].index,
                    'Adj_Close': combined_df['Close'][ticker].values,
                    'Volume': combined_df['Volume'][ticker].values
                }).reset_index(drop=True)
                
                filename = f"{data_dir}/{ticker}.parquet"
                ticker_data.to_parquet(filename, index=False)
                print(f"Saved {ticker}: {len(ticker_data)} records to {filename}")
                
            except Exception as e:
                print("Error")

    def load_price(self, ticker: str = 'AMD', start_date: str = '2020-01-01', end_date: str = '2025-01-01', data_dir: str = 'data'):

        all_close_data = {}
        all_volume_data = {}

        if ticker == '*':
            for file_name in os.listdir(data_dir):
                if file_name.endswith('.parquet'):
                    ticker = file_name.replace('.parquet', '')
                    file_path = os.path.join(data_dir, file_name)

                    df = pd.read_parquet(file_path)
                    df = df.set_index('Date')
                    all_close_data[ticker] = df['Adj_Close']
                    all_volume_data[ticker] = df['Volume']

        else:
            file_path = os.path.join(data_dir, f"{ticker}.parquet")
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Missing data file: {file_path}")

            df = pd.read_parquet(os.path.join(data_dir, f"{ticker}.parquet"))
            df = df.set_index('Date')
            all_close_data[ticker] = df['Adj_Close']
            all_volume_data[ticker] = df['Volume']

        combined_close_df = pd.DataFrame(all_close_data)
        combined_volume_df = pd.DataFrame(all_volume_data)

        return {'Close': combined_close_df, 'Volume': combined_volume_df}

    def long_format(self, total_df):

        long_df = total_df['Close'].reset_index().melt(
            id_vars=['Date'],  # Keep Date as identifier
            var_name='symbol',  # Name the variable column 'symbol'
            value_name='price'  # Name the value column 'price'
        )

        long_volume_df = total_df['Volume'].reset_index().melt(
            id_vars=['Date'],  # Keep Date as identifier
            var_name='symbol',  # Name the variable column 'symbol'
            value_name='volume'  # Name the value column 'volume'
        )

        long_df = long_df.merge(
            long_volume_df[['Date', 'symbol', 'volume']], 
            on=['Date', 'symbol'], 
            how='left'
        )

        long_df = long_df.sort_values(['Date', 'symbol']).reset_index(drop=True)

        return long_df
        