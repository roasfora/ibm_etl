import os
import pandas as pd

# Diretórios corretos dentro do projeto Airflow
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
OUTPUT_DIR = os.path.join(DATA_DIR, 'processed')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Caminhos dos arquivos CSV
daily_path = os.path.join(DATA_DIR, 'ibm_daily_raw.csv')
overview_path = os.path.join(DATA_DIR, 'ibm_overview_raw.csv')

# Carregar CSVs
cotacoes_df = pd.read_csv(daily_path)
overview_df = pd.read_csv(overview_path)

# Mostrar colunas disponíveis (debug)
print("📄 Colunas de overview_df:", overview_df.columns.tolist())

# Padronizar colunas para evitar erro por capitalização
overview_df.columns = [col.strip().lower() for col in overview_df.columns]

# Criar dim_tempo
dim_tempo = cotacoes_df[['date']].drop_duplicates().copy()
dim_tempo['date'] = pd.to_datetime(dim_tempo['date'])
dim_tempo['tempo_id'] = dim_tempo['date'].dt.strftime('%Y%m%d')
dim_tempo['dia'] = dim_tempo['date'].dt.day
dim_tempo['mes'] = dim_tempo['date'].dt.month
dim_tempo['ano'] = dim_tempo['date'].dt.year
dim_tempo['trimestre'] = dim_tempo['date'].dt.to_period('Q').astype(str)
dim_tempo = dim_tempo[['tempo_id', 'date', 'dia', 'mes', 'trimestre', 'ano']]
dim_tempo.to_csv(os.path.join(OUTPUT_DIR, 'dim_tempo.csv'), index=False)

# Criar dim_empresa com validação
dim_empresa = pd.DataFrame([{
    "empresa_id": 1,
    "simbolo": overview_df.loc[0, 'symbol'] if 'symbol' in overview_df.columns else 'N/A',
    "nome": overview_df.loc[0, 'name'] if 'name' in overview_df.columns else 'N/A',
    "setor": overview_df.loc[0, 'sector'] if 'sector' in overview_df.columns else 'N/A',
    "industria": overview_df.loc[0, 'industry'] if 'industry' in overview_df.columns else 'N/A',
    "pais": overview_df.loc[0, 'country'] if 'country' in overview_df.columns else 'N/A',
}])
dim_empresa.to_csv(os.path.join(OUTPUT_DIR, 'dim_empresa.csv'), index=False)

# Criar dim_indicador
indicadores = [
    (1, 'EPS', 'Lucro por ação'),
    (2, 'DividendPerShare', 'Dividendo por ação'),
    (3, 'PERatio', 'Preço sobre Lucro'),
    (4, 'ReturnOnEquityTTM', 'Retorno sobre patrimônio líquido'),
    (5, 'MarketCapitalization', 'Valor de mercado')
]
dim_indicador = pd.DataFrame(indicadores, columns=["indicador_id", "nome_indicador", "descricao"])
dim_indicador.to_csv(os.path.join(OUTPUT_DIR, 'dim_indicador.csv'), index=False)

# Criar fact_cotacoes
cotacoes_df['tempo_id'] = pd.to_datetime(cotacoes_df['date']).dt.strftime('%Y%m%d')
fact_cotacoes = cotacoes_df[['tempo_id', '1. open', '2. high', '3. low', '4. close', '5. volume']].copy()
fact_cotacoes.insert(0, 'cotacao_id', range(1, len(fact_cotacoes) + 1))
fact_cotacoes.insert(2, 'empresa_id', 1)
fact_cotacoes.columns = [
    'cotacao_id', 'tempo_id', 'empresa_id', 'preco_abertura', 'preco_maximo',
    'preco_minimo', 'preco_fechamento', 'volume'
]
fact_cotacoes.to_csv(os.path.join(OUTPUT_DIR, 'fact_cotacoes.csv'), index=False)

# Criar fact_indicadores com função segura
def safe_get(col_name):
    col_name = col_name.lower()
    return overview_df.loc[0, col_name] if col_name in overview_df.columns else None

fact_indicadores = pd.DataFrame([
    [1, '20241231', 1, 1, safe_get('EPS')],
    [2, '20241231', 1, 2, safe_get('DividendPerShare')],
    [3, '20241231', 1, 3, safe_get('PERatio')],
    [4, '20241231', 1, 4, safe_get('ReturnOnEquityTTM')],
    [5, '20241231', 1, 5, safe_get('MarketCapitalization')],
], columns=['indicador_fato_id', 'tempo_id', 'empresa_id', 'indicador_id', 'valor'])

fact_indicadores.to_csv(os.path.join(OUTPUT_DIR, 'fact_indicadores.csv'), index=False)

print("✅ Transformações concluídas! Arquivos salvos em:", OUTPUT_DIR)
