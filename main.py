import yfinance as yf
from pprint import pprint

data = yf.Ticker("AAPL")
pprint(data.info)