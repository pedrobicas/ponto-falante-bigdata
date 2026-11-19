-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Camada Ouro - Modelagem Dimensional, LGPD e 5 Perguntas Analíticas

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_linha AS
SELECT DISTINCT
  linha AS id_linha,
  sentido,
  acessivel
FROM prata_passagens;

CREATE OR REPLACE TABLE dim_ponto AS
SELECT DISTINCT
  ponto AS id_ponto
FROM prata_passagens;

CREATE OR REPLACE TABLE dim_tempo AS
SELECT DISTINCT
  to_date(atualizado_em) AS data_ref,
  hour(atualizado_em) AS hora,
  dayofweek(atualizado_em) AS dia_semana,
  date_format(atualizado_em, 'EEEE') AS nome_dia_semana
FROM prata_passagens;

CREATE OR REPLACE TABLE fato_passagem AS
SELECT
  sha2(coalesce(veiculo_id, 'anonimo'), 256) AS veiculo_hash,
  ponto AS id_ponto,
  linha AS id_linha,
  to_date(atualizado_em) AS data_ref,
  hour(atualizado_em) AS hora,
  minutos AS atraso_minutos,
  acessivel,
  fonte
FROM prata_passagens;

-- CONSULTA 1: Quais linhas mais atrasam em relação ao horário programado?
SELECT
  id_linha,
  fonte,
  ROUND(AVG(atraso_minutos), 2) AS atraso_medio_min,
  COUNT(*) AS total_passagens
FROM fato_passagem
GROUP BY id_linha, fonte
ORDER BY atraso_medio_min DESC;

-- CONSULTA 2: Em quais faixas de horário a espera é maior?
SELECT
  hora AS faixa_horaria,
  ROUND(AVG(atraso_minutos), 2) AS espera_media_min,
  COUNT(*) AS qtd_registros
FROM fato_passagem
GROUP BY hora
ORDER BY espera_media_min DESC;

-- CONSULTA 3: Chuva / Fonte de dados aumenta o atraso dos ônibus?
SELECT
  fonte,
  ROUND(AVG(atraso_minutos), 2) AS atraso_medio_min,
  MAX(atraso_minutos) AS pior_atraso_min,
  COUNT(*) AS amostras
FROM fato_passagem
GROUP BY fonte;

-- CONSULTA 4: Quantas linhas que passam em um ponto têm veículo acessível?
SELECT
  id_ponto,
  COUNT(DISTINCT CASE WHEN acessivel = true THEN id_linha END) AS linhas_acessiveis,
  COUNT(DISTINCT id_linha) AS total_linhas_ponto
FROM fato_passagem
GROUP BY id_ponto
ORDER BY linhas_acessiveis DESC;

-- CONSULTA 5: Qual a variação do tempo de espera ao longo da semana?
SELECT
  t.dia_semana,
  t.nome_dia_semana,
  ROUND(AVG(f.atraso_minutos), 2) AS media_espera_min
FROM fato_passagem f
JOIN dim_tempo t ON f.data_ref = t.data_ref AND f.hora = t.hora
GROUP BY t.dia_semana, t.nome_dia_semana
ORDER BY t.dia_semana;
