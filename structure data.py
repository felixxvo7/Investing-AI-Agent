import pandas as pd

# Load Data
balance_sheet = pd.read_csv("balance_sheet.csv")
cashflow = pd.read_csv("cashflow.csv")
financials = pd.read_csv("financials.csv")

# Company Metadata
TICKER = "AAPL"
COMPANY_NAME = "Apple Inc."

# Function to unpivot and add metadata
def melt_financial_df(df, source_name):
    df = df.melt(id_vars=["Metrics"], var_name="Year", value_name="Value")
    df["Company Ticker"] = TICKER
    df["Company Name"] = COMPANY_NAME
    df["Source"] = source_name
    return df[["Year", "Company Ticker", "Company Name", "Source", "Metrics", "Value"]]

# Process each file
balance_long = melt_financial_df(balance_sheet, "Balance Sheet")
cashflow_long = melt_financial_df(cashflow, "Cashflow")
financials_long = melt_financial_df(financials, "Financial Statement")

# Combine all into one structured DataFrame
combined = pd.concat([balance_long, cashflow_long, financials_long])

# Optional: Pivot to wide format (metrics as columns)
structured_df = combined.pivot_table(
    index=["Year", "Company Ticker", "Company Name"],
    columns=["Source", "Metrics"],
    values="Value",
    aggfunc="first"
).reset_index()

# Optional: Flatten MultiIndex columns
structured_df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in structured_df.columns]

# Save as CSV or Excel
structured_df.to_csv("structured_financials.csv", index=False)
structured_df.to_excel("structured_financials.xlsx", index=False)

print("✅ Structured file saved as 'structured_financials.csv' and 'structured_financials.xlsx'")
