# Projeto Ponto Falante — Pipeline de Dados e Análise de Atrasos (SPTrans + Clima)

Global Solution 2026.2 · CitizenTech · Engenharia de Software, 4º ano (FIAP) · Big Data Architecture & Data Integration.

Solução de Engenharia de Big Data para monitorar e analisar a pontualidade do transporte público em São Paulo em relação às condições climáticas, combinando ingestão/orquestração no **Apache Airflow** e processamento **Medallion** no **Databricks**.

## Integrantes

| Nome completo | RM |
|---|---|
| Pedro Henrique Bicas Couto | 99534 |
| Bryan Willian | 551305 |
| Felipe Masera Terra | 99405 |
| Gabriel Fernandes Doms | 98630 |
| Lucas Vassão Braga | 98607 |

## Links

- Relatório em PDF: [`docs/Relatorio_BigData_Ponto_Falante.pdf`](docs/Relatorio_BigData_Ponto_Falante.pdf)
- Produto (MVP publicado): https://ponto-falante.onrender.com
- Repositório principal do produto: https://github.com/pedrobicas/ponto-falante

## Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `dags/` | DAG do Airflow (`pipeline_ponto_falante`) |
| `databricks/` | Notebooks e SQL das camadas Bronze, Prata e Ouro |
| `amostras/` | Um pouco de cada fonte (SPTrans e Open-Meteo) |
| `chegada/` | Pasta de pouso onde a DAG grava os arquivos coletados |
| `docs/` | Relatório em PDF |

## Visão Geral e Arquitetura

- **Fontes de dados:** API Olho Vivo SPTrans (ou simulador de fallback quando não há token ou disponibilidade) + API climática Open-Meteo.
- **Orquestração:** Apache Airflow com a DAG `pipeline_ponto_falante`, gravando payloads JSON em `chegada/` e apoiada por arquivos/exemplos em `amostras/`.
- **Lakehouse (Databricks):**
  - **Bronze (`bronze.transporte_bruto`):** carga bruta preservando histórico e origem (`olhovivo` ou `simulador`).
  - **Prata (`prata.transporte_tratado`):** limpeza, filtros de qualidade (ex.: exclusão de linha de teste `999X-99` e inconsistências extremas de atraso), padronização e metadados.
  - **Ouro (`ouro.fato_passagem` e tabelas analíticas):** estrutura pronta para consultas SQL e governança LGPD com hashing `SHA-256` (`64` caracteres).

## Como Rodar

1. **Subir o Airflow:** Na raiz do projeto, execute `docker-compose up -d` e abra `http://localhost:8080` no navegador.
2. **Rodar a DAG de coleta:** Ative e dispare a DAG `pipeline_ponto_falante`.
   - **Sem token da SPTrans:** dispare normalmente para usar o simulador automático junto aos dados do Open-Meteo (os arquivos saem em `chegada/`).
   - **Com token da SPTrans:** use *Trigger DAG w/ config* passando `{"sptrans_token": "SEU_TOKEN_AQUI"}`.
3. **Processar no Databricks:** Importe `01_bronze.py`, `02_prata.py` e `03_ouro.py`, envie os dados de `chegada/` e execute os notebooks nessa ordem.
4. **Executar as consultas SQL:** Rode as consultas analíticas no Databricks sobre as tabelas geradas para conferir chuva, atrasos, faixas de temperatura, pontos/frota e auditoria `SHA-256`.
