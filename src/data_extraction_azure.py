import os
import requests
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

API_KEY = os.getenv("API_KEY")
SYMBOL = "IBM"
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")


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


def upload_to_blob(df, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_name)
    csv_data = df.to_csv(index=False).encode('utf-8')
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"✔️ Arquivo enviado para o Azure Blob: {blob_name}")


if __name__ == "__main__":
    # Extração
    df_daily = extract_daily_data(SYMBOL)
    df_overview = extract_company_overview(SYMBOL)

    # Upload direto para o Azure
    upload_to_blob(df_daily, 'ibm_daily_raw.csv')
    upload_to_blob(df_overview, 'ibm_overview_raw.csv')

    print("🏁 Dados salvos diretamente no Azure Blob Storage com sucesso!")
