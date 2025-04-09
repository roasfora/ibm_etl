# 🧠 Projeto ETL com Alpha Vantage - IBM

Este projeto realiza a **extração, transformação e carga (ETL)** de dados financeiros da empresa **IBM**, utilizando a API da **Alpha Vantage**. O objetivo é estruturar esses dados em um **modelo dimensional**, ideal para análise em ferramentas de BI ou projetos de ciência de dados.

---

## 📁 Estrutura do Projeto


```text
projeto_etl_ibm/
├── data/
│   ├── ibm_daily_raw.csv         # Dados brutos de cotações
│   ├── ibm_overview_raw.csv      # Dados brutos corporativos
│   └── processed/
│       ├── dim_tempo.csv         # Dimensão temporal
│       ├── dim_empresa.csv       # Dimensão empresa
│       ├── dim_indicador.csv     # Dimensão indicadores
│       ├── fact_cotacoes.csv     # Fato cotações
│       └── fact_indicadores.csv  # Fato indicadores
├── doc/
│   └── ETL.png                   # Diagrama do processo
├── data_extraction.py            # Script de extração
├── data_transformation.py        # Script de transformação
└── README.md                     # Documentação do projeto



---

## 🚀 Pipeline ETL

### 🔹 1. Extração (`data_extraction.py`)
- Consulta de preços diários da ação IBM via `TIME_SERIES_DAILY`
- Consulta de dados fundamentais via `OVERVIEW`

### 🔹 2. Transformação (`data_transformation.py`)
Geração das seguintes tabelas:

#### 📘 Dimensões:
- `dim_tempo`: informações temporais (data, mês, ano, trimestre)
- `dim_empresa`: dados estáticos da IBM (nome, setor, país, etc.)
- `dim_indicador`: indicadores financeiros selecionados

#### 📗 Fatos:
- `fact_cotacoes`: preços de abertura, fechamento, volume diário
- `fact_indicadores`: EPS, P/L, Dividendos, ROE, Market Cap

---

## 🧪 Como Executar

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/projeto_etl_ibm.git
cd projeto_etl_ibm
