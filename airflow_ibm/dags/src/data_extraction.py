import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

API_KEY = os.getenv("API_KEY")
SYMBOL = "IBM"

# Diretório do projeto Airflow onde os dados serão salvos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

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

# Extração de dados fundamentais da empresa (Overview)
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

# Execução principal
if __name__ == "__main__":
    print("📡 Iniciando extração de dados...")

    # Definindo os caminhos dos arquivos de saída
    daily_output_path = os.path.join(DATA_DIR, 'ibm_daily_raw.csv')
    overview_output_path = os.path.join(DATA_DIR, 'ibm_overview_raw.csv')

    # Extração
    df_daily = extract_daily_data(SYMBOL)
    df_overview = extract_company_overview(SYMBOL)

    # Salvando os arquivos
    df_daily.to_csv(daily_output_path, index=False)
    df_overview.to_csv(overview_output_path, index=False)

    print("✅ Extração finalizada com sucesso!")
    print(f"📄 Dados diários salvos em: {daily_output_path}")
    print(f"📄 Dados gerais salvos em: {overview_output_path}")
