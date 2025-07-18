import pandas as pd
import yfinance as yf

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
    "Cash Flow From Continuing Financing Activities",
    "Capital Expenditure"
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

def extract_balance(fin: "yf.Ticker") -> pd.DataFrame:
    # 1) Pull tables
    df_std = fin.balance_sheet

    # 2) Validate
    if df_std is None or df_std.empty:
        raise ValueError("No annual/quarterly balance sheet data available.")

    balance_values = get_std_values(df_std, balance_metrics)
    
    # print(f'Extracted balance values:\n{balance_values}')
    return balance_values

def extract_cashflow(fin: "yf.Ticker") -> pd.DataFrame:
    # 1) Pull tables
    df_std = fin.cashflow

    # 2) Validate
    if df_std is None or df_std.empty:
        raise ValueError("No annual/quarterly cash flow data available.")

    cashflow_values = get_std_values(df_std, cashflow_metrics)
    
    # print(f'Extracted cash flow values:\n{cashflow_values}')
    return cashflow_values

if __name__ == '__main__':
    aapl = yf.Ticker('AAPL')
    fin = extract_financial(aapl)
    balance = extract_balance(aapl)
    cashflow = extract_cashflow(aapl)
    
    print('Converting data to CSV files...\n')
    fin.to_csv('dataset/financials.csv', index=False)
    balance.to_csv('dataset/balance_sheet.csv', index=False)
    cashflow.to_csv('dataset/cashflow.csv', index=False)
    print('Data conversion complete!')
