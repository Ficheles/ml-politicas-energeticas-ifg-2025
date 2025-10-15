# Análise e Refatoração do DAG: inmet_data_to_snowflake_dbt_etl-loop-years

## 📊 Problema Identificado

### ❌ Abordagem Anterior (Problemática)

O código original tinha uma **arquitetura híbrida incorreta**:

```python
def generate_copy_tasks(**context):
    # Criava SQLExecuteQueryOperator DENTRO da função Python
    for filename in files:
        copy_task = SQLExecuteQueryOperator(...)  # ❌ Criado em runtime
        # Tentava fazer chaining, mas não executava
```

**Problemas:**
1. ✗ Operadores SQL criados **em tempo de execução** (quando `generate_copy_tasks` rodava)
2. ✗ Airflow requer que o **DAG seja definido estaticamente** no parse time
3. ✗ Tasks criadas dinamicamente **não eram executadas** automaticamente
4. ✗ Dependencies (>>) definidas dentro da função não funcionavam corretamente

### 🔍 Fluxo Anterior (Quebrado)

```
Ano 2000-2025 (loop):
  ├─ list_s3_files_{year}        ← ✅ Executava boto3, retornava lista via XCom
  ├─ generate_copy_tasks_{year}   ← ⚠️  Executava Python, CRIAVA tasks
  └─ [Tasks COPY criadas mas NÃO executadas] ❌
```

**Resultado:** Os arquivos eram listados, mas o COPY INTO nunca executava!

---

## ✅ Solução Implementada

### Refatoração com TaskFlow API + Dynamic Task Mapping

Implementei a abordagem moderna do Airflow 2.3+ usando:

1. **@task decorators** - TaskFlow API para funções Python
2. **Dynamic Task Mapping** - `.expand()` para processar múltiplos itens
3. **Proper XCom handling** - Passagem automática de dados entre tasks

### 🎯 Novo Fluxo (Correto)

```
create_file_format
      |
      v
┌─────────────────────────────────────────────┐
│  Ano 2000                                   │
│  ├─ list_s3_files(2000)                     │
│  │    └─ Retorna: [{file1}, {file2}, ...]   │
│  └─ copy_file_to_snowflake.expand()         │
│       ├─ Task[0]: COPY file1  ✓             │
│       ├─ Task[1]: COPY file2  ✓             │
│       └─ Task[n]: COPY fileN  ✓             │
└─────────────────────────────────────────────┘
      |
      v
┌─────────────────────────────────────────────┐
│  Ano 2001                                   │
│  ├─ list_s3_files(2001)                     │
│  └─ copy_file_to_snowflake.expand()         │
│       └─ [Tasks dinâmicas por arquivo]      │
└─────────────────────────────────────────────┘
      |
      v
    ...
      |
      v
┌─────────────────────────────────────────────┐
│  Ano 2025                                   │
│  ├─ list_s3_files(2025)                     │
│  └─ copy_file_to_snowflake.expand()         │
│       └─ [Tasks dinâmicas por arquivo]      │
└─────────────────────────────────────────────┘
```

---

## 🔧 Mudanças Técnicas Implementadas

### 1. Import do TaskFlow API

```python
from airflow.decorators import task
```

### 2. Função `list_s3_files` Refatorada

**Antes:**
```python
def list_s3_files(bucket, base_prefix, year, **context):
    # Retornava string com newlines
    return '\n'.join(files) if files else ""
```

**Depois:**
```python
@task
def list_s3_files(bucket: str, base_prefix: str, year: int):
    # Retorna lista de dicts com metadados
    return [
        {'filename': 'file1.CSV', 'year': 2000, 'size': 12345},
        {'filename': 'file2.CSV', 'year': 2000, 'size': 67890},
        ...
    ]
```

**Benefícios:**
- ✅ Type hints para melhor documentação
- ✅ Retorna estrutura de dados rica (dict) em vez de string
- ✅ Decorator @task gerencia XCom automaticamente
- ✅ Metadados (size, year) passados para próxima task

### 3. Nova Função `copy_file_to_snowflake`

```python
@task
def copy_file_to_snowflake(file_info: dict):
    """Execute Snowflake COPY INTO para um único arquivo CSV."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    year = file_info['year']
    filename = file_info['filename']
    
    # Constrói e executa SQL
    sql = f"""
        COPY INTO {FULLY_QUALIFIED_TABLE}
        FROM (...)
        FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGE}.STAGE_RAW/{year}/{filename}
        (FILE_FORMAT => '{FULLY_QUALIFIED_FILE_FORMAT}')
    """
    
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    result = hook.run(sql, autocommit=True)
    
    return {'filename': filename, 'status': 'success', ...}
```

**Características:**
- ✅ Processa **um arquivo por vez** (task isolada)
- ✅ Usa SnowflakeHook para execução direta
- ✅ Logging detalhado por arquivo
- ✅ Retorna métricas de processamento
- ✅ Tratamento de erro isolado por arquivo

### 4. Dynamic Task Mapping com `.expand()`

**Código:**
```python
for year in YEARS:
    # Lista arquivos (retorna list[dict])
    files_list = list_s3_files(bucket=bucket_name, base_prefix=initial_prefix, year=year)
    
    # MAGIC: .expand() cria UMA TASK PARA CADA ITEM DA LISTA
    copy_results = copy_file_to_snowflake.expand(file_info=files_list)
    
    # Dependency chaining
    create_file_format >> files_list >> copy_results
```

**Como funciona `.expand()`:**
- Airflow pega a lista retornada por `list_s3_files`
- Cria **dinamicamente** uma task `copy_file_to_snowflake` para cada item
- Tasks são nomeadas automaticamente: `copy_file_to_snowflake[0]`, `copy_file_to_snowflake[1]`, etc.
- Cada task recebe **um dict** do array como parâmetro `file_info`

---

## 📈 Benefícios da Refatoração

### ✅ Execução Garantida
- **Antes:** Tasks criadas mas não executadas
- **Depois:** Cada arquivo gera uma task que executa automaticamente

### ✅ Visibilidade no Airflow UI
- **Antes:** Apenas `generate_copy_tasks` aparecia (sem detalhes)
- **Depois:** Cada arquivo tem sua própria task visível no Graph View

### ✅ Paralelização (Potencial)
- **Antes:** Impossível paralelizar
- **Depois:** Tasks podem rodar em paralelo (configurável via `max_active_tasks`)

### ✅ Retry Isolado
- **Antes:** Falha em um arquivo quebrava tudo
- **Depois:** Retry granular por arquivo

### ✅ Logging Estruturado
- Cada arquivo tem seu próprio log
- Métricas individuais: tempo de processamento, linhas carregadas
- Rastreabilidade completa

### ✅ Type Safety
- Type hints para parâmetros
- Validação automática pelo Airflow

---

## 🎯 Verificação do Fluxo

### Como garantir que CADA arquivo é processado:

1. **list_s3_files** retorna: `[{file1}, {file2}, {file3}]`
2. **.expand()** cria automaticamente:
   - `copy_file_to_snowflake[0]` → processa `{file1}`
   - `copy_file_to_snowflake[1]` → processa `{file2}`
   - `copy_file_to_snowflake[2]` → processa `{file3}`
3. **Airflow scheduler** executa cada task
4. **SnowflakeHook** executa o COPY INTO para cada arquivo

### Diagrama de Dependências:

```
create_file_format
        |
        v
  list_s3_files_2000  →  [array com 5 arquivos]
        |
        v
copy_file_to_snowflake[0]  ← arquivo 1
copy_file_to_snowflake[1]  ← arquivo 2
copy_file_to_snowflake[2]  ← arquivo 3
copy_file_to_snowflake[3]  ← arquivo 4
copy_file_to_snowflake[4]  ← arquivo 5
        |
        v
  list_s3_files_2001  →  [array com 3 arquivos]
        |
        v
copy_file_to_snowflake[0]  ← arquivo 1
copy_file_to_snowflake[1]  ← arquivo 2
copy_file_to_snowflake[2]  ← arquivo 3
```

---

## 📝 Exemplo de Execução

### Logs Esperados:

```
[list_s3_files_2006]
[YEAR 2006] Starting S3 file listing...
[YEAR 2006] Found file: INMET_CO_GO_A002_GOIANIA.CSV (size: 1234567 bytes)
[YEAR 2006] Found file: INMET_CO_GO_A011_SAO_SIMAO.CSV (size: 987654 bytes)
[YEAR 2006] S3 listing completed: 2 CSV files found

[copy_file_to_snowflake[0]]
[YEAR 2006] Processing file: INMET_CO_GO_A002_GOIANIA.CSV
[YEAR 2006] S3 path: inmet/2006/INMET_CO_GO_A002_GOIANIA.CSV
[YEAR 2006] File size: 1234567 bytes
[YEAR 2006] File processed successfully in 2.45s
[YEAR 2006] Query result: [('success', 8760)]  ← linhas carregadas

[copy_file_to_snowflake[1]]
[YEAR 2006] Processing file: INMET_CO_GO_A011_SAO_SIMAO.CSV
[YEAR 2006] S3 path: inmet/2006/INMET_CO_GO_A011_SAO_SIMAO.CSV
[YEAR 2006] File size: 987654 bytes
[YEAR 2006] File processed successfully in 1.89s
[YEAR 2006] Query result: [('success', 7320)]
```

---

## 🚀 Próximos Passos (Melhorias Futuras)

### 1. Paralelização
```python
copy_results = copy_file_to_snowflake.expand(
    file_info=files_list,
    max_active_tasks=10  # Processar 10 arquivos simultaneamente
)
```

### 2. Captura de Métricas do Snowflake
```python
# Parsear o resultado do COPY INTO
result = hook.run(sql)
rows_loaded = int(result[0][1])  # Extrair do result set
return {'rows_loaded': rows_loaded, 'status': 'success'}
```

### 3. Task Group por Ano
```python
from airflow.utils.task_group import TaskGroup

with TaskGroup(f"year_{year}") as year_group:
    files_list = list_s3_files(...)
    copy_results = copy_file_to_snowflake.expand(...)
```

### 4. Tratamento de Erros Avançado
```python
@task(retries=3, retry_delay=timedelta(minutes=5))
def copy_file_to_snowflake(file_info: dict):
    try:
        # ... COPY INTO
    except Exception as e:
        logger.error(f"Failed to process {filename}: {e}")
        # Enviar alerta, registrar falha, etc.
        raise
```

---

## 📚 Referências

- [Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Snowflake Hook](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html)

---

**Data:** 2025-10-15  
**Autor:** GitHub Copilot  
**Status:** ✅ Implementado e Testado
