# Import functions
from pyspark.sql.functions import current_date

# Create schema
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schemaName}')

# Drop table
spark.sql(f'DROP TABLE IF EXISTS {schemaName}.{tableName}')

# Read data
df = spark.read.parquet(f"Files/{schemaName}/{filePath}/{tableName}.parquet")

# Add metadata loading_date column using current date
df = df.withColumn("loading_date", current_date().cast("string"))

# Overwrite table
df.write.mode("Overwrite").saveAsTable(f"{schemaName}.{tableName}")
