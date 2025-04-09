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

![ETL IBM Pipeline](https://raw.githubusercontent.com/roasfora/ibm_etl/master/doc/ETL.png)
