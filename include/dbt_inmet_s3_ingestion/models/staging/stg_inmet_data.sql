{{
    config(
        materialized='view'
    )
}}

-- Select and rename raw columns, converting the comma decimal separator to a period.
SELECT
    -- Date and Time Keys
    TRY_TO_DATE(T1."Data", 'YYYY/MM/DD') AS DATA_OBSERVACAO,
    TRY_TO_TIME(REPLACE(T1."Hora UTC", ' UTC', '')) AS HORA_OBSERVACAO,
    
    -- Replace comma (,) with period (.) for decimal conversion
    TRY_TO_NUMERIC(REPLACE(T1."PRECIPITAÇÃO TOTAL, HORÁRIO (mm)", ',', '.')) AS PRECIPITACAO_MM,
    TRY_TO_NUMERIC(REPLACE(T1."PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)", ',', '.')) AS PRESSAO_ATM_ESTACAO_MB,
    TRY_TO_NUMERIC(REPLACE(T1."PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)", ',', '.')) AS PRESSAO_ATM_MAX_MB,
    TRY_TO_NUMERIC(REPLACE(T1."PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)", ',', '.')) AS PRESSAO_ATM_MIN_MB,
    TRY_TO_NUMERIC(REPLACE(T1."RADIACAO GLOBAL (Kj/m²)", ',', '.')) AS RADIACAO_GLOBAL_KJ_M2,
    TRY_TO_NUMERIC(REPLACE(T1."TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)", ',', '.')) AS TEMP_AR_BULBO_SECO_C,
    TRY_TO_NUMERIC(REPLACE(T1."TEMPERATURA DO PONTO DE ORVALHO (°C)", ',', '.')) AS TEMP_PONTO_ORVALHO_C,
    TRY_TO_NUMERIC(REPLACE(T1."TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)", ',', '.')) AS TEMP_MAX_C,
    TRY_TO_NUMERIC(REPLACE(T1."TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)", ',', '.')) AS TEMP_MIN_C,
    
    -- Humidity and Wind
    T1."UMIDADE RELATIVA DO AR, HORARIA (%)" AS UMIDADE_REL_HORARIA_PCT,
    TRY_TO_NUMERIC(REPLACE(T1."VENTO, RAJADA MAXIMA (m/s)", ',', '.')) AS VENTO_RAJADA_MAX_MS,
    TRY_TO_NUMERIC(REPLACE(T1."VENTO, VELOCIDADE HORARIA (m/s)", ',', '.')) AS VENTO_VELOCIDADE_HORARIA_MS
    
FROM
    {{ source('raw', 'stg_inmet_data') }} AS T1
    -- NOTE: In a real scenario, you'd use a more explicit column access (e.g., T1.$1, T1.$2...) 
    -- if loading the raw table as a single VARIANT column (which is common practice for staging).
    -- For simplicity and clarity, this code assumes the COPY INTO created columns from the header.
    -- If using a VARIANT column, you must use a lateral flatten join to extract the columns.
