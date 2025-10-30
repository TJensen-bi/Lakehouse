# Fetch parameters from Azure Data Factory
#schemaName=dbutils.widgets.get("schemaName")
#tableName=dbutils.widgets.get("tableName")
#filePath=dbutils.widgets.get("filePath")

schemaName="adventureworks"
tableName="Address"
filePath="20241022"

# Fetch schema data from landing zone
jsonSchema = spark.read.parquet(f"/Volumes/buildingma/volumes/mnt_landing/\
{schemaName}/{filePath}/{tableName}.parquet").schema.json()
ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType \
  .fromJson(jsonSchema).toDDL()

# Migrate parquet data to delta files
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.includeExistingFiles", "true")
  .option("cloudFiles.backfillInterval", "1 day")
  .option("cloudFiles.schemaLocation", f"/Volumes/buildingma/volumes/mnt_landing/\
  {schemaName}/_checkpoint/{tableName}_autoload/")
  .schema(ddl)
  .load(f"/Volumes/buildingma/volumes/mnt_landing/\
  {schemaName}/{filePath}/{tableName}.parquet")
  .writeStream
  .format("delta")
  .option("checkpointLocation", f"/Volumes/buildingma/volumes/mnt_landing/\
  {schemaName}/_checkpoint/{tableName}_autoload/")
  .trigger(availableNow=True)
  .toTable(f"bronze_adventureworks.{tableName}")
)
