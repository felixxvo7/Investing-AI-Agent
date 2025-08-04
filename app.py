import yfinance as yf
import pandas as pd
from pprint import pprint

data = yf.download('AAPL', start='2020-07-31', end='2025-07-31')

df = data.to_csv('./dataset/stock_price.csv')
print('Done')
