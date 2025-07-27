import pandas as pd
import yfinance as yf
from pathlib import Path
import sys

MAX_TICKERS = 10

# 1) Define your metrics
financial_metrics = [
    "Total Revenue",
    "Cost Of Revenue",       # COGS
    "EBIT",                  # Operating Income
    "Net Income",
    # EPS isn't in financials DF → we'll grab from .info
    "Research And Development"   # R&D Expense
]

balance_metrics = [
    "Total Assets",
    "Current Assets",
    "Inventory",
    "Total Liabilities Net Minority Interest",
    "Current Liabilities",
    "Long Term Debt",
    "Total Debt",
    "Stockholders Equity"           # Shareholders’ Equity
]

cashflow_metrics = [
    "Cash Flow From Continuing Operating Activities",
    "Capital Expenditure"
]

market_metrics = [
    'currentPrice',
    'sharesOutstanding',
    'forwardEps',
    'enterpriseValue'
]

def get_std_values(df, metrics) -> pd.DataFrame:
    available_metrics = df.index.intersection(metrics)
    df_slice = df.loc[available_metrics].copy()
    df_slice.index.name = "Metrics"
    df_slice = df_slice.reset_index()
    df_slice.columns = [
        col
        if col in ("Metrics", "TTM")
        else pd.to_datetime(col).date().isoformat()
        for col in df_slice.columns
    ]
    return df_slice

def extract_financial(fin: "yf.Ticker") -> pd.DataFrame:
    # 1) Pull tables
    df_std = fin.financials
    df_ttm = fin.ttm_financials

    # 2) Validate
    if df_std is None or df_std.empty:
        raise ValueError("No annual/quarterly financial data available.")
    if df_ttm is None or df_ttm.empty:
        raise ValueError("No TTM financial data available.")
    
    # 3) Extract TTM values
    latest_col = df_ttm.columns[0]
    ttm_values = {
        m: df_ttm.at[m, latest_col]
        for m in financial_metrics
        if m in df_ttm.index
    }
    df_ttm_slice = (
        pd.DataFrame.from_dict(ttm_values, orient="index", columns=["TTM"])
          .rename_axis("Metrics")
          .reset_index()
    )

    df_std = get_std_values(df_std, financial_metrics)

    financial_values = pd.merge(df_ttm_slice, df_std, on="Metrics", how="outer")

    # print(f'Extracted financial values:\n{financial_values}')
    return financial_values

def extract_balance(bs: "yf.Ticker") -> pd.DataFrame:
    # 1) Pull tables
    df_std = bs.balance_sheet

    # 2) Validate
    if df_std is None or df_std.empty:
        raise ValueError("No annual/quarterly balance sheet data available.")

    balance_values = get_std_values(df_std, balance_metrics)
    
    # print(f'Extracted balance values:\n{balance_values}')
    return balance_values

def extract_cashflow(cf: "yf.Ticker") -> pd.DataFrame:
    # 1) Pull tables
    df_std = cf.cashflow
    df_ttm = cf.ttm_cashflow
    
    # 2) Validate
    if df_std is None or df_std.empty:
        raise ValueError("No annual/quarterly cash flow data available.")
    if df_ttm is None or df_ttm.empty:
        raise ValueError("No annual/quarterly TTM cash flow data available.")
    
    # 3) Extract TTM values
    latest_col = df_ttm.columns[0]
    ttm_values = {
        m: df_ttm.at[m, latest_col]
        for m in cashflow_metrics
        if m in df_ttm.index
    }   
    df_ttm_slice = (
        pd.DataFrame.from_dict(ttm_values, orient="index", columns=["TTM"])
          .rename_axis("Metrics")
          .reset_index()
    )
    
    df_std = get_std_values(df_std, cashflow_metrics)

    cashflow_values = pd.merge(df_ttm_slice, df_std, on="Metrics", how="outer")
    
    return cashflow_values

def extract_market(market: "yf.Ticker") -> pd.DataFrame:
    # 1) Pull market data
    info = market.info

    # 2) Validate
    if not info:
        raise ValueError("No market data available.")

    market_values = {
        'currentPrice': info.get('currentPrice', None),
        'sharesOutstanding': info.get('sharesOutstanding', None),
        'forwardEps': info.get('forwardEps', None),
        'enterpriseValue': info.get('enterpriseValue', None)
    }

    df_market = pd.DataFrame.from_dict(market_values, orient='index', columns=['Value'])
    df_market.index.name = "Metrics"
    df_market.reset_index(inplace=True)

    return df_market

# Ticker is validated by having a regular market price
def is_valid_ticker(ticker):
    try:
        info = yf.Ticker(ticker).info
        return 'regularMarketPrice' in info and info['regularMarketPrice'] is not None
    except:
        return False

if __name__ == '__main__':
    # Allow user to enter an amount of MAX_TICKERS
    while True:
        try:
            num_tickers = int(input(f"Enter number of stock tickers to process (max {MAX_TICKERS}): "))
            if 1 <= num_tickers <= MAX_TICKERS:
                break
            else:
                print(f"Please enter a number between 1 and {MAX_TICKERS}.")
        except ValueError:
            print("Invalid input. Please enter a number.")
    
    stock_tickers = []
    for _ in range(num_tickers):
        stock = input("Enter stock ticker (e.g., AAPL): ").strip().upper()
        
        if not is_valid_ticker(stock):
            print(f"Invalid or unknown ticker: {stock}")
            sys.exit(1)
        
        stock_tickers.append(stock)

    # Create Tickers object after validation
    stock_tickers_obj = yf.Tickers(" ".join(stock_tickers))

    # Ensure 'dataset/' folder exists
    Path("dataset").mkdir(exist_ok=True)

    for stock in stock_tickers:
        ticker_obj = stock_tickers_obj.tickers[stock]
        
        # Define file paths
        files = {
            "financials": Path(f"dataset/{stock}_financials.csv"),
            "balance": Path(f"dataset/{stock}_balance_sheet.csv"),
            "cashflow": Path(f"dataset/{stock}_cashflow.csv"),
            "market": Path(f"dataset/{stock}_market.csv"),
        }

        # If all files exist, skip
        if all(f.exists() for f in files.values()):
            print(f"All data for {stock} already exists. Skipping...")
            continue

        print(f"Fetching and saving data for {stock}...")

        # Extract data
        fin = extract_financial(ticker_obj)
        balance = extract_balance(ticker_obj)
        cashflow = extract_cashflow(ticker_obj)
        market = extract_market(ticker_obj)

        # Save only missing files
        if not files["financials"].exists():
            fin.to_csv(files["financials"], index=False)

        if not files["balance"].exists():
            balance.to_csv(files["balance"], index=False)

        if not files["cashflow"].exists():
            cashflow.to_csv(files["cashflow"], index=False)

        if not files["market"].exists():
            market.to_csv(files["market"], index=False)

        print(f"Saved data for {stock}\n")
    
    print('All done!')