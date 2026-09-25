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

`CotacaoMoedaPeriodo` (parâmetros: `moeda`, `dataInicial`, `dataFinalCotacao`)

O endpoint disponibiliza as cotações de compra e venda por moeda e período, contemplando os boletins publicados durante o dia:

|Boletim|Quantidade|
|---|--:|
|Abertura|1|
|Intermediário|3|
|Fechamento|1|
|**Total**|**5 por dia útil**|

A exploração da fonte confirmou que o endpoint `CotacaoMoedaPeriodo` possui granularidade superior aos endpoints `CotacaoDolarDia`, `CotacaoDolarPeriodo` e "Boletim por data" (descartados por redundância — representam apenas a cotação PTAX de fechamento, já contida no endpoint escolhido).

### Escopo de moedas

Todas as **10 moedas** disponíveis no endpoint `Moedas` (AUD, CAD, CHF, DKK, EUR, GBP, JPY, NOK, SEK, USD). Volume estimado: ~50 linhas/dia útil. O endpoint `Moedas` (catálogo com símbolo/nome/tipo) é candidato a uma tabela de referência futura — não implementada ainda.

### Catalog e schemas (Unity Catalog)

Criado no Databricks Free Edition:

```text
cambio_radar (catalog)
├── bronze (schema)
│   └── cotacoes_ptax (tabela Delta)
└── silver (schema)
```

### Características da fonte

- Dados públicos
    
- Sem autenticação
    
- Formato estruturado
    
- Atualização apenas em dias úteis (confirmado empiricamente — sem publicação em fim de semana/feriado)
    
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

Camada **Gold**: ainda não decidida.

### Bronze

Camada responsável pela ingestão dos dados diretamente da API.

Características:

- Preserva os dados retornados pela fonte, sem transformação nem validação de tipo (100% crua)
    
- Utiliza estratégia de `append`
    
- Grava o dado mesmo quando a captura vem incompleta (moeda com menos de 5 boletins, ou moeda que falhou na chamada) — serve como evidência para manutenção/análise
    
- Armazena os dados em Delta Lake
    

**Colunas:** `run_id` (UUID, identifica a execução inteira do pipeline), `moeda`, `paridadeCompra`, `paridadeVenda`, `cotacaoCompra`, `cotacaoVenda`, `dataHoraCotacao`, `tipoBoletim`, `insert_dt` (timestamp de captura)

**Fluxo de ingestão (implementado):**

1. Gera `run_id` (UUID) e `insert_dt` (timestamp de captura), antes de qualquer request
2. Gap-detection: consulta a Bronze dos últimos 30 dias por (moeda, data), contando `COUNT(DISTINCT dataHoraCotacao)`
3. Monta `pares_pendentes`: um único intervalo `(moeda, data_inicial, hoje)` por moeda — `data_inicial` é a primeira data incompleta encontrada (cobre gaps internos) ou o dia seguinte ao último dia completo; para moeda sem nenhum dado nos últimos 30 dias, assume backfill inicial de 30 dias (padrão ajustável)
4. Faz um request por par pendente (até 10 chamadas, uma por moeda, cada uma cobrindo o intervalo necessário)
5. Confere sucesso/falha por moeda; enriquece cada boletim retornado com `moeda`, `run_id`, `insert_dt`
6. Insere os dados que vieram (mesmo que parcial) na Bronze via `append` em Delta — com proteção para o caso de todas as chamadas falharem (nada a gravar)
7. Checa o agregado de sucesso/falha — se houver falha, dispara `raise Exception`, fazendo a task falhar e acionar o alerta nativo do Databricks Jobs

Não há tabela de log de execução persistida — decisão explícita, por não se justificar no estágio atual do projeto (revisitar se o projeto crescer, ex. dashboard de saúde do pipeline). O histórico de execuções fica a cargo do próprio Databricks Jobs.

### Silver

Camada responsável pelo tratamento e disponibilização dos dados para consumo posterior.

Principais responsabilidades:

- Validação estrutural e de tipo dos campos (não feita na Bronze)
    
- Deduplicação
    
- Controle de unicidade
    
- Operações de `upsert`
    

**Schema (`cambio_radar.silver.cotacoes_ptax`):** `moeda`, `dataHoraCotacao` (TIMESTAMP), `tipoBoletim`, `sequencia_intermediario` (INT, só preenchido para boletins Intermediário), `cotacaoCompra`, `cotacaoVenda`, `paridadeCompra`, `paridadeVenda`, `run_id`, `insert_dt` (TIMESTAMP)

**Fluxo de transformação (implementado):**

1. Consulta `MAX(insert_dt)` já processado na própria Silver (sem tabela de log — mesmo princípio da Bronze)
2. Lê da Bronze apenas o que tem `insert_dt` maior que o último processado (watermark por momento de processamento, não por data de negócio — evita o mesmo problema que motivou o watermark da Bronze, já que um reprocessamento pode gravar uma data de negócio antiga)
3. Aplica `to_timestamp()` nas colunas `dataHoraCotacao` e `insert_dt` (gravadas como STRING na Bronze, confirmado via `DESCRIBE`)
4. Checagem de qualidade: se algum `dataHoraCotacao` ficou `NULL` após o cast (indício de mudança no formato da fonte), dispara `raise Exception` — `to_timestamp()` não gera erro sozinho para formato não reconhecido, só retorna nulo silenciosamente
5. Calcula `sequencia_intermediario` via `ROW_NUMBER()` particionado por (moeda, data) e ordenado por `dataHoraCotacao`, aplicado somente às linhas com `tipoBoletim = 'Intermediário'`
6. Grava via `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`, usando `(moeda + dataHoraCotacao)` como chave — esse par já é suficiente para unicidade (o timestamp completo, com hora, já distingue os 3 boletins Intermediário); a sequência fica só como coluna analítica, fora da chave de unicidade

Não há checagem equivalente de nulo para `insert_dt` — decisão deliberada, já que é um campo gerado internamente (não vem da fonte), sem risco de mudança de formato externo.
    

---

## Desafio de unicidade e deduplicação (resolvido)

Durante a exploração da API foi identificado um comportamento relevante para o desenho da camada Silver.

O campo `tipoBoletim` utiliza o valor **"Intermediário"** para três boletins diferentes publicados no mesmo dia. A API não fornece uma numeração explícita para diferenciar o primeiro, segundo e terceiro boletim intermediário.

Por isso, uma chave composta apenas por:

```text
data + moeda + tipoBoletim
```

não representa unicamente cada cotação. Uma operação de `MERGE` baseada nessa chave poderia substituir registros válidos sem gerar necessariamente um erro explícito, resultando em perda silenciosa de dados.

**Decisão adotada:** gerar um número sequencial na transformação (ordenando por `dataHoraCotacao` dentro de cada data+moeda) e usar `(data + moeda + tipoBoletim + sequência)` como chave de unicidade na Silver.

**Nota sobre a implementação real:** ao construir a Silver, ficou claro que `dataHoraCotacao` completo (com hora) já é único por boletim, incluindo os 3 Intermediários. Por isso, a chave usada no `MERGE` acabou sendo `(moeda + dataHoraCotacao)`, mais simples — a sequência permanece útil como coluna analítica (filtrar "todos os 2º Intermediários", por exemplo), mas não é mais necessária para garantir unicidade.

---

## Watermark e detecção de gaps

Mecanismo de definição de qual período buscar a cada execução, cobrindo tanto **dias inteiros perdidos** (ex.: job não rodou) quanto **moedas parcialmente perdidas dentro de um dia com execução parcial**.

1. Consulta a própria Bronze dos últimos 30 dias, agrupando por (data, moeda) e contando `COUNT(DISTINCT dataHoraCotacao)`
2. Qualquer combinação com menos de 5 entra numa lista de pendências a reprocessar
3. A busca da execução atual = pendências + próximo dia após o último dia completo

`dataHoraCotacao` é o campo usado na contagem (em vez de `COUNT(*)` ou `COUNT(DISTINCT tipoBoletim)`) porque: é um carimbo de publicação vindo da fonte, então mantém os 3 boletins "Intermediário" como valores distintos, e ao mesmo tempo absorve naturalmente duplicatas geradas por reprocessamento (mesmo boletim, mesmo `dataHoraCotacao`) — sem precisar de log nem de coluna de flag de reprocesso.

Não há tabela de log de execução, nem coluna explícita marcando reprocessamento — a própria Bronze funciona como fonte de verdade para ambos.

**Implementação real:** em vez de um par `(moeda, data)` por data faltante, o mecanismo gera **um único intervalo por moeda** (aproveitando que o endpoint aceita período) — uma moeda com gap num dia específico e que também precisa buscar hoje recebe uma única chamada cobrindo o intervalo inteiro. Reprocessar dias já completos dentro desse intervalo é inofensivo, pois `dataHoraCotacao` absorve a duplicata.

---

## Orquestração e notificação

- Ferramenta de orquestração: **Databricks Jobs** (decisão fechada, sem restrição de custo identificada até o momento)
- Célula final do notebook lê o agregado de sucesso/falha por moeda; se houver falha, executa `raise Exception`, fazendo a task falhar explicitamente
- Job criado no Databricks Jobs com task do tipo Notebook (Source = Workspace — o job sempre executa a versão mais recente salva no notebook, sem necessidade de reconfiguração a cada edição)
- Agendamento diário configurado; execução manual ("Run now") também disponível a qualquer momento, sem configuração adicional
- Notificação por e-mail configurada para falha (`on failure`); não há alerta de conclusão/sucesso — decisão deliberada, por ser um processo agendado sem necessidade de notificar "deu tudo certo"
- **Validado com teste real:** falha forçada via timeout gerou exceção, marcou a task como Failed e disparou o e-mail de alerta corretamente. Teste com código de moeda inválido (`XXX`) não gera falha — a API responde HTTP 200 com lista vazia para moeda inexistente, em vez de erro; comportamento aceito como não sendo um risco real, já que a lista de moedas usada pelo pipeline é fixa e validada

**Dependência Bronze → Silver:** as duas camadas vivem no mesmo Job (não existe trigger nativo entre Jobs totalmente separados no Databricks sem código customizado). A task da Silver tem "Depends on" apontando para a task da Bronze, com condição "All succeeded" — a Silver só executa se a Bronze terminar com sucesso, evitando processar sobre uma Bronze desatualizada ou incompleta. Testado com execução real do Job completo: as duas tasks concluíram com sucesso em sequência.

**Nota sobre alternativas avaliadas:** o Lakeflow Declarative Pipelines (antigo Delta Live Tables) resolveria boa parte dessa lógica de forma declarativa (incremental automático, dependências inferidas, sem necessidade de watermark manual). Decisão foi terminar a versão manual primeiro, já que o objetivo do projeto é validar autonomia de arquitetura from scratch — migrar para lá é considerado como possível exercício comparativo futuro.

---

## Fluxo de processamento

```text
API PTAX
   │
   ▼
Extração
   │
   ├── Watermark / gap-detection (últimos 30 dias na Bronze)
   ├── 1 request por moeda (10)
   └── Confere sucesso/falha por moeda
   │
   ▼
Bronze
   │
   ├── Dados brutos (mesmo que parciais)
   └── Delta Lake
   │
   ▼
Alerta (se houve falha) — Databricks Jobs
   │
   ▼  (task dependency: "All succeeded")
Silver
   │
   ├── Watermark por insert_dt (o que é novo desde o último processamento)
   ├── Cast de tipo + checagem de nulo pós-cast
   ├── Sequenciamento (Intermediário) + MERGE (moeda + dataHoraCotacao)
   └── Delta Lake
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
|Orquestração|Databricks Jobs|
|Testes e monitoramento|Alerta nativo de falha (Databricks Jobs), validado com teste real de timeout; sem log de execução persistido|

---

## Qualidade de dados

A estratégia de qualidade considera, entre outros pontos:

- Validação do schema recebido (camada Silver)
    
- Controle de registros duplicados
    
- Validação da granularidade da fonte (5 boletins por dia útil)
    
- Verificação da quantidade esperada de boletins via watermark/gap-detection
    
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
    
-  Definir chave definitiva de unicidade (sequencial + data + moeda + tipoBoletim)
    
-  Definir escopo de moedas (todas as 10)
    
-  Estruturar o repositório
    
-  Desenhar a ingestão Bronze (colunas, fluxo, controle de execução)
    
-  Desenhar o mecanismo de watermark / detecção de gaps
    
-  Definir estratégia de orquestração e notificação de falhas
    
-  Criar catalog e schemas no Unity Catalog (`cambio_radar.bronze`, `cambio_radar.silver`)
    
-  Implementar e validar a ingestão Bronze ponta a ponta (request por moeda, gap-detection, backfill automático, escrita Delta) — primeira execução completa das 10 moedas gravou 1050 linhas com sucesso
    
-  Implementar e validar o alerta de falha real (raise Exception + notificação por e-mail do Databricks Jobs) — testado com falha forçada de timeout, e-mail recebido corretamente
    
-  Configurar o agendamento do job no Databricks Jobs (execução diária + Run now disponível)
    
-  Implementar e validar a transformação Silver (watermark por insert_dt, cast de tipo, checagem de nulo, sequenciamento, MERGE) — testado com dado real
    
-  Configurar dependência Bronze → Silver no mesmo Job ("Depends on", All succeeded) — testado com execução completa, ambas as tasks com sucesso
    

### Em desenvolvimento

-  Implementar testes de qualidade
    
-  Documentar decisões arquiteturais (dicionário de dados)
    

---

## Próximas etapas

1. Implementar validações de qualidade
2. Concluir o dicionário de dados
3. Avaliar a necessidade da tabela de catálogo de moedas
4. Avaliar a necessidade de uma camada Gold
5. (Futuro) Reconstruir o pipeline com Lakeflow Declarative Pipelines, como exercício comparativo manual vs. declarativo

---

## Estrutura do repositório

```text
Cambio_Radar_PTAX/
├── README.md
├── Scripts_Notebooks/
│   ├── Python/          (exploração avulsa, ex.: testes de API)
│   └── Databricks/
├── Pipeline/
│   ├── Bronze/
│   │   ├── Python/      (funções reutilizáveis, ex.: chamada à API)
│   │   └── Databricks/  (notebook que orquestra/chama as funções)
│   └── Silver/
│       ├── Python/
│       └── Databricks/
└── Dicionario_de_Dados/ (em elaboração)
```

Camada Gold ainda não criada — decisão pendente.

---
