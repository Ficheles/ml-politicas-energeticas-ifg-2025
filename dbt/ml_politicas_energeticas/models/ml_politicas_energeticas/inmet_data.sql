{{ config(
    materialized='table',
    tags=['inmet', 'raw', 's3']
) }}

with source as (

    select *
    from s3(
        '{{ var("inmet_s3_path", "https://ml-politicas-energeticas.s3.us-east-2.amazonaws.com/inmet/2025/INMET_CO_GO_A002_GOIANIA_01-01-2024_A_31-12-2024.CSV") }}',
        '{{ env_var("AWS_ACCESS_KEY_ID", "") }}',
        '{{ env_var("AWS_SECRET_ACCESS_KEY", "") }}',
        'CSVWithNames',
        -- A estrutura continua lendo todas as colunas como String, o que é perfeito para este caso.
        '
        "Data" String,
        "Hora UTC" String,
        "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)" String,
        "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)" String,
        "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)" String,
        "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)" String,
        "RADIACAO GLOBAL (Kj/m²)" String,
        "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)" String,
        "TEMPERATURA DO PONTO DE ORVALHO (°C)" String,
        "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)" String,
        "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)" String,
        "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)" String,
        "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)" String,
        "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)" String,
        "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)" String,
        "UMIDADE RELATIVA DO AR, HORARIA (%)" String,
        "VENTO, DIREÇÃO HORARIA (gr) (° (gr))" String,
        "VENTO, RAJADA MAXIMA (m/s)" String,
        "VENTO, VELOCIDADE HORARIA (m/s)" String
        '
    )
    SETTINGS
        input_format_csv_skip_first_lines = 8,
        format_csv_delimiter = ';'

)

-- Seleciona todas as colunas, renomeando para nomes mais amigáveis,
-- mas sem fazer NENHUM cast ou conversão.
select
    "Data" as data,
    "Hora UTC" as hora_utc,
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)" as precipitacao_mm,
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)" as pressao_atm_estacao_mb,
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)" as pressao_atm_max_mb,
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)" as pressao_atm_min_mb,
    "RADIACAO GLOBAL (Kj/m²)" as radiacao_global_kj_m2,
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)" as temperatura_ar_c,
    "TEMPERATURA DO PONTO DE ORVALHO (°C)" as temperatura_orvalho_c,
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)" as temperatura_max_c,
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)" as temperatura_min_c,
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)" as temperatura_orvalho_max_c,
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)" as temperatura_orvalho_min_c,
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)" as umidade_rel_max_pct,
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)" as umidade_rel_min_pct,
    "UMIDADE RELATIVA DO AR, HORARIA (%)" as umidade_rel_ar_pct,
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))" as vento_direcao_gr,
    "VENTO, RAJADA MAXIMA (m/s)" as vento_rajada_max_ms,
    "VENTO, VELOCIDADE HORARIA (m/s)" as vento_velocidade_ms
from source
