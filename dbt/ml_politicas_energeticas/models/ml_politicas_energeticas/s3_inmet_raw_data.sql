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
        '
        "Data" String,
        "Hora UTC" String,
        "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)" Nullable(Float32),
        "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)" Nullable(Float32),
        "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)" Nullable(Float32),
        "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)" Nullable(Float32),
        "RADIACAO GLOBAL (Kj/m²)" Nullable(Float32),
        "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)" Nullable(Float32),
        "TEMPERATURA DO PONTO DE ORVALHO (°C)" Nullable(Float32),
        "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)" Nullable(Float32),
        "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)" Nullable(Float32),
        "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)" Nullable(Float32),
        "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)" Nullable(Float32),
        "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)" Nullable(Float32),
        "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)" Nullable(Float32),
        "UMIDADE RELATIVA DO AR, HORARIA (%)" Nullable(Float32),
        "VENTO, DIREÇÃO HORARIA (gr) (° (gr))" Nullable(Int32),
        "VENTO, RAJADA MAXIMA (m/s)" Nullable(Float32),
        "VENTO, VELOCIDADE HORARIA (m/s)" Nullable(Float32)
        ',
        'input_format_csv_skip_first_lines = 8, format_csv_delimiter = \';\''
    )

)

select
    cast("Data" as Date) as date,
    "Hora UTC" as hour,
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)" as precipitacao,
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)" as temperatura,
    "UMIDADE RELATIVA DO AR, HORARIA (%)" as umidade,
    "VENTO, VELOCIDADE HORARIA (m/s)" as vento
from source
