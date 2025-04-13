import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carrega variáveis do arquivo .env
load_dotenv()

# Lê as variáveis
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
DBNAME = os.getenv("DB_NAME")
PORT = os.getenv("DB_PORT")

# Monta a URL de conexão
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(DATABASE_URL)

# Caminhos dos CSVs (como fizemos antes)
csv_files = {
    "dim_empresa": "C:/Users/isabe/Desktop/EDIT/EDIT/Data Engineering/projeto_etl_ibm/data/processed/dim_empresa.csv",
    "dim_indicador": "C:/Users/isabe/Desktop/EDIT/EDIT/Data Engineering/projeto_etl_ibm/data/processed/dim_indicador.csv",
    "dim_tempo": "C:/Users/isabe/Desktop/EDIT/EDIT/Data Engineering/projeto_etl_ibm/data/processed/dim_tempo.csv",
    "fact_cotacoes": "C:/Users/isabe/Desktop/EDIT/EDIT/Data Engineering/projeto_etl_ibm/data/processed/fact_cotacoes.csv",
    "fact_indicadores": "C:/Users/isabe/Desktop/EDIT/EDIT/Data Engineering/projeto_etl_ibm/data/processed/fact_indicadores.csv"
}

def create_tables(csv_files, engine):
    for table_name, file_path in csv_files.items():
        print(f"Carregando {file_path} na tabela {table_name}...")
        df = pd.read_csv(file_path)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Tabela {table_name} criada com sucesso!")

if __name__ == "__main__":
    create_tables(csv_files, engine)
