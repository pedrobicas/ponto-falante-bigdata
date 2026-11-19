spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Leitura incremental com Auto Loader (CloudFiles)
df_bronze = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .load("/Volumes/workspace/default/chegada/")

query = df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/bronze") \
    .toTable("bronze.transporte_bruto")

print("=== BRONZE INCREMENTAL EXECUTADA ===")