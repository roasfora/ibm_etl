import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Carrega variáveis do .env
load_dotenv()

# Conexão com o banco
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
DBNAME = os.getenv("DB_NAME")
PORT = os.getenv("DB_PORT")

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(DATABASE_URL)

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    print("Conexão bem-sucedida! Resultado:", result.scalar())
