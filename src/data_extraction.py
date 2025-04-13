import os
import requests
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

API_KEY = os.getenv("API_KEY")
SYMBOL = "IBM"

# Extração dos dados históricos diários (preços e volume)
def extract_daily_data(symbol):
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'full',
        'apikey': API_KEY
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    time_series = data.get('Time Series (Daily)', {})

    df = pd.DataFrame.from_dict(time_series, orient='index').reset_index()
    df.rename(columns={'index': 'date'}, inplace=True)

    return df

# Extração de dados fundamentais gerais da empresa (Overview)
def extract_company_overview(symbol):
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'OVERVIEW',
        'symbol': symbol,
        'apikey': API_KEY
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    return pd.DataFrame([data])

if __name__ == "__main__":
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    daily_output_path = os.path.join(output_dir, 'ibm_daily_raw.csv')
    overview_output_path = os.path.join(output_dir, 'ibm_overview_raw.csv')

    df_daily = extract_daily_data(SYMBOL)
    df_overview = extract_company_overview(SYMBOL)

    df_daily.to_csv(daily_output_path, index=False)
    df_overview.to_csv(overview_output_path, index=False)

    print(" Extração finalizada com sucesso!\n"
          f" Dados diários salvos em: {daily_output_path}\n"
          f" Dados gerais salvos em: {overview_output_path}")
