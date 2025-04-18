import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Diretórios corretos dentro do projeto Airflow
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, '..', 'data', 'processed')

# Conexão com o banco
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
DBNAME = os.getenv("DB_NAME")
PORT = os.getenv("DB_PORT")

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(DATABASE_URL)

# Caminhos dos arquivos CSV
csv_files = {
    "dim_empresa": os.path.join(PROCESSED_DIR, "dim_empresa.csv"),
    "dim_indicador": os.path.join(PROCESSED_DIR, "dim_indicador.csv"),
    "dim_tempo": os.path.join(PROCESSED_DIR, "dim_tempo.csv"),
    "fact_cotacoes": os.path.join(PROCESSED_DIR, "fact_cotacoes.csv"),
    "fact_indicadores": os.path.join(PROCESSED_DIR, "fact_indicadores.csv")
}

# Função principal de carga
def create_tables(csv_files, engine):
    for table_name, file_path in csv_files.items():
        print(f"\nCarregando dados para a tabela `{table_name}`...")

        if not os.path.exists(file_path):
            print(f"Arquivo não encontrado: {file_path}")
            continue

        df = pd.read_csv(file_path)

        # Limpa a tabela antes de inserir (sem derrubar estrutura)
        with engine.begin() as conn:
            print(f"Limpando dados anteriores da tabela `{table_name}`...")
            conn.execute(text(f"DELETE FROM {table_name}"))

        # Insere os dados
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Tabela `{table_name}` atualizada com sucesso.")

# Execução
if __name__ == "__main__":
    print("Iniciando carga dos dados no banco PostgreSQL...\n")
    create_tables(csv_files, engine)
    print("\nCarga finalizada com sucesso.")
