# Projeto Goiás Renovável - ml-politicas-energeticas-ifg-2025
PROJETO — MÓDULO 2: Machine Learning para Políticas Públicas Energéticas - POS IA IFG 2025/1

## Arquitetura do Pipeline de Dados e MLOps

Aqui está o fluxo de trabalho automatizado para o projeto de potencial energético, desde a extração de dados até o treinamento do modelo.

```mermaid
graph TD
    subgraph "Fontes de Dados Externas"
        A["Dados em Arquivos<br/>- ANEEL (Usinas CSV)<br/>- IBGE (Shapefiles)<br/>- INMET (CSVs Meteorológicos)<br/>- ONS (Shapefiles de Rede)<br/>- IMB (PIB CSV)"]
    end

    subgraph "Orquestração e Execução"
        B("Apache Airflow<br/>Orquestrador Central")
    end

    subgraph "Camada de Armazenamento (Cloud - AWS S3)"
        S3_RAW["S3 Data Lake<br/>(Dados Brutos)"]
        S3_MODELS["S3 Model Registry<br/>(Modelos Treinados .pkl)"]
    end

    subgraph "Data Warehouse (Processamento e Análise)"
        CH("ClickHouse")
        CH_RAW["Tabelas Brutas<br/>raw_aneel, raw_inmet..."]
        CH_MART["Tabela Analítica<br/>mart_microrregiao_potencial"]
        CH --- CH_RAW & CH_MART
    end
    
    subgraph "Lógica de Negócio e ML"
        C["Script de Ingestão<br/>(Python/GeoPandas)"]
        D["dbt (data build tool)<br/>(Transformação SQL)"]
        E["Script de Treinamento ML<br/>(Python/Scikit-learn/XGBoost)"]
    end

    subgraph "Consumo dos Resultados"
        F["Dashboards & Análises<br/>(Power BI, Metabase, etc.)"]
        G["API de Inferência<br/>(Servindo o modelo)"]
    end

    %% FLUXO DO PIPELINE
    B -->|1. Dispara Script| C
    A -->|2. Lê arquivos| C
    C -->|3. Salva dados brutos| S3_RAW
    S3_RAW -->|4. Carrega no DWH| CH_RAW

    B -->|5. Dispara Transformação| D
    CH_RAW -->|6. dbt lê dados brutos| D
    D -->|7. dbt cria tabela analítica| CH_MART

    B -->|8. Dispara Treinamento| E
    CH_MART -->|9. Lê dados de treino| E
    E -->|10. Salva modelo treinado| S3_MODELS

    CH_MART -->|Análise| F
    S3_MODELS -->|Deploy| G
```


# Activate DBT env


```bash
 conda activate ml-dbt
```

conda deactivate