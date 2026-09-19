# Câmbio Radar

Pipeline incremental para ingestão e processamento de cotações de câmbio da **API PTAX do Banco Central do Brasil**, utilizando Databricks, PySpark e Delta Lake.

O projeto implementa uma arquitetura de dados em camadas, com foco em **ingestão incremental, processamento, deduplicação, qualidade dos dados e preparação para execução recorrente**.

## Objetivo

Construir um pipeline de dados ponta a ponta a partir de uma fonte pública real, documentada e atualizada em dias úteis.

A fonte utilizada é a API PTAX, que disponibiliza as cotações oficiais de câmbio do Banco Central, incluindo os diferentes boletins publicados ao longo do dia.

O pipeline tem como foco:

- Ingestão de dados via API REST/OData
    
- Paginação e tratamento de erros
    
- Processamento incremental
    
- Armazenamento em Delta Lake
    
- Arquitetura Medallion (Bronze/Silver)
    
- Deduplicação e controle de unicidade
    
- Validação da qualidade dos dados
    
- Orquestração de cargas recorrentes
    
- Versionamento do código com Git
    

---

## Fonte de dados

**API PTAX — Banco Central do Brasil**

Documentação: [Olinda — Open Data do Banco Central](https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/documentacao)

### Endpoint

`CotacaoMoedaPeriodo`

O endpoint disponibiliza as cotações de compra e venda por moeda e período, contemplando os boletins publicados durante o dia:

|Boletim|Quantidade|
|---|--:|
|Abertura|1|
|Intermediário|3|
|Fechamento|1|
|**Total**|**5 por dia útil**|

A exploração da fonte confirmou que o endpoint `CotacaoMoedaPeriodo` possui granularidade superior aos endpoints `CotacaoDolarDia` e `CotacaoDolarPeriodo`, que representam a cotação PTAX de fechamento.

### Características da fonte

- Dados públicos
    
- Sem autenticação
    
- Formato estruturado
    
- Atualização em dias úteis
    
- Dados intradiários
    
- Informações de data e horário da cotação
    
- Suporte a múltiplas moedas
    

---

## Arquitetura

```text
                    ┌──────────────────────┐
                    │      API PTAX        │
                    │  Banco Central       │
                    └──────────┬───────────┘
                               │
                               │ Ingestão
                               ▼
                    ┌──────────────────────┐
                    │       BRONZE         │
                    │                      │
                    │ Dados brutos         │
                    │ Append                │
                    │ Delta Lake           │
                    └──────────┬───────────┘
                               │
                               │ Transformação
                               ▼
                    ┌──────────────────────┐
                    │       SILVER         │
                    │                      │
                    │ Dados tratados       │
                    │ Deduplicação         │
                    │ Upsert                │
                    │ Delta Lake           │
                    └──────────────────────┘
```

### Bronze

Camada responsável pela ingestão dos dados diretamente da API.

Características:

- Preserva os dados retornados pela fonte
    
- Utiliza estratégia de `append`
    
- Mantém informações relacionadas à captura
    
- Não aplica regras de negócio
    
- Armazena os dados em Delta Lake
    

### Silver

Camada responsável pelo tratamento e disponibilização dos dados para consumo posterior.

Principais responsabilidades:

- Validação estrutural
    
- Deduplicação
    
- Tratamento dos dados
    
- Controle de unicidade
    
- Operações de `upsert`
    

A estratégia definitiva de chave de unicidade está em definição devido à granularidade específica dos boletins da API.

---

## Desafio de unicidade e deduplicação

Durante a exploração da API foi identificado um comportamento relevante para o desenho da camada Silver.

O campo `tipoBoletim` utiliza o valor **"Intermediário"** para três boletins diferentes publicados no mesmo dia. A API não fornece uma numeração explícita para diferenciar o primeiro, segundo e terceiro boletim intermediário.

Por isso, uma chave composta apenas por:

```text
data + moeda + tipoBoletim
```

não representa unicamente cada cotação.

Uma operação de `MERGE` baseada nessa chave poderia substituir registros válidos sem gerar necessariamente um erro explícito, resultando em perda silenciosa de dados.

A estratégia de unicidade está sendo avaliada com base nos campos de data/hora disponibilizados pela própria fonte.

---

## Fluxo de processamento

```text
API PTAX
   │
   ▼
Extração
   │
   ├── Paginação
   ├── Tratamento de erros
   └── Captura dos dados
   │
   ▼
Bronze
   │
   ├── Dados brutos
   └── Delta Lake
   │
   ▼
Silver
   │
   ├── Validação
   ├── Deduplicação
   └── Upsert
   │
   ▼
Dados tratados
```

---

## Stack técnica

|Categoria|Tecnologia|
|---|---|
|Plataforma|Databricks|
|Linguagem|Python|
|Processamento|PySpark|
|Armazenamento|Delta Lake|
|Arquitetura|Medallion / Bronze / Silver|
|Fonte|API PTAX / OData|
|Versionamento|Git|
|Orquestração|Em implementação|
|Testes e monitoramento|Em implementação|

---

## Qualidade de dados

A estratégia de qualidade considera, entre outros pontos:

- Validação do schema recebido
    
- Controle de registros duplicados
    
- Validação da granularidade da fonte
    
- Verificação da quantidade esperada de boletins
    
- Integridade das informações de data e hora
    
- Identificação de inconsistências durante a ingestão
    

As regras serão incorporadas progressivamente ao pipeline conforme as camadas forem implementadas.

---

## Status

🟡 **Em desenvolvimento**

### Concluído

-  Validar acesso do Databricks à API externa
    
-  Explorar os endpoints disponíveis
    
-  Identificar o endpoint principal de ingestão
    
-  Confirmar a granularidade de 5 boletins por dia útil
    
-  Identificar o comportamento dos boletins intermediários
    
-  Identificar o problema de unicidade para a camada Silver
    

### Em desenvolvimento

-  Definir chave definitiva de unicidade
    
-  Definir escopo de moedas
    
-  Estruturar o repositório
    
-  Implementar ingestão Bronze
    
-  Implementar transformação Silver
    
-  Implementar deduplicação
    
-  Implementar upsert
    
-  Implementar orquestração
    
-  Implementar testes de qualidade
    
-  Implementar monitoramento
    
-  Documentar decisões arquiteturais
    

---

## Próximas etapas

1. Definir a chave de unicidade da camada Silver
    
2. Implementar a ingestão incremental na Bronze
    
3. Implementar as transformações da Silver
    
4. Implementar validações de qualidade
    
5. Implementar execução recorrente
    
6. Adicionar monitoramento e tratamento de falhas
    
7. Avaliar a necessidade de uma camada Gold
    

---

## Estrutura prevista

```text
cambio-radar/
│
├── notebooks/
│   ├── bronze/
│   └── silver/
│
├── src/
│   ├── ingestion/
│   ├── transformation/
│   └── validation/
│
├── tests/
│
├── docs/
│
└── README.md
```

A estrutura será ajustada conforme a implementação evoluir.

---

## Projeto relacionado

**AssistBR** — projeto de Data Warehouse dimensional desenvolvido em paralelo, com foco em modelagem dimensional, regras de negócio e arquitetura analítica.

Enquanto o AssistBR explora principalmente modelagem e arquitetura de Data Warehouse, o Câmbio Radar concentra-se no ciclo de vida de um pipeline de dados: **ingestão → processamento → armazenamento → qualidade → orquestração**.