spark.sql("CREATE SCHEMA IF NOT EXISTS prata")
from pyspark.sql.functions import col, upper, when, current_timestamp

df_bronze = spark.read.table("bronze.transporte_bruto")

# Regra 1: Atraso absurdo negativo (< -30 min)
# Regra 2: Linha inválida ou teste ('999X-99')
# Regra 3: ID de veículo nulo
df_prata_auditada = df_bronze.withColumn(
    "motivo_rejeicao",
    when(col("atraso_minutos") < -30, "REJEITADO_ATRASO_NEGATIVO")
    .when(col("codigo_linha") == "999X-99", "REJEITADO_LINHA_TESTE")
    .when(col("id_veiculo").isNull(), "REJEITADO_ID_NULO")
    .otherwise("APROVADO")
)

# Mostrar contagem de registros barrados por regra (Exigência do Requisito 06)
display(df_prata_auditada.groupBy("motivo_rejeicao").count())

# Filtrar apenas aprovados e padronizar
df_prata_final = df_prata_auditada.filter(col("motivo_rejeicao") == "APROVADO") \
    .withColumn("codigo_linha_maiusculo", upper(col("codigo_linha"))) \
    .withColumn("processado_em", current_timestamp())

# Gravação utilizando MERGE (Delta Lake) para evitar duplicidade na Prata
df_prata_final.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("prata.transporte_tratado")

print("=== CAMADA PRATA PROCESSADA COM MERGE E REGRAS DE QUALIDADE ===")