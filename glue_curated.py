import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws, year, month, date_format
from pyspark.sql.types import DoubleType

#Parâmetros
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATASET', 'ANO'])
dataset = args['DATASET']
ano_ref = int(args['ANO'])

#Inicialização
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Leitura com mergeSchema
input_path = f"s3://gpe-ingestao/processed/{dataset}/"
print(f"Lendo dados de {input_path}")
df = spark.read.option("mergeSchema", "true").parquet(input_path)

#Debug do schema
schema_cols = df.schema.names
print("Schema lido:")
df.printSchema()

#Define colunas de tempo por dataset
if dataset == "yellow":
    pickup_col = "tpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime"
elif dataset == "green":
    pickup_col = "lpep_pickup_datetime"
    dropoff_col = "lpep_dropoff_datetime"
elif dataset == "fhvhv":
    pickup_col = "request_datetime"
    dropoff_col = "dropoff_datetime"
else:
    pickup_col = "pickup_datetime"
    dropoff_col = "dropoff_datetime"

#Função auxiliar para colunas opcionais
def safe_col(name, tipo="string"):
    return col(name) if name in schema_cols else lit(None).cast(tipo)

#Seleção das colunas disponíveis
df_curated = df.select(
    col(pickup_col).alias("pickup_datetime"),
    col(dropoff_col).alias("dropoff_datetime"),
    safe_col("trip_distance", "double").alias("trip_distance"),
    safe_col("fare_amount", "double").alias("fare_amount"),
    safe_col("total_amount", "double").alias("total_amount"),
    col("ano").alias("year"),
    col("mes").alias("month")
)

#Garante que colunas existam para uso posterior
for col_name in ["trip_distance", "fare_amount", "total_amount"]:
    if col_name not in df_curated.columns:
        df_curated = df_curated.withColumn(col_name, lit(None).cast("double"))

#Colunas adicionais
df_curated = df_curated.withColumn("calendar_id", date_format(col("pickup_datetime"), "yyyy-MM-dd")) \
                       .withColumn("type_id", lit(dataset)) \
                       .withColumn("trip_id", sha2(
                           concat_ws("||",
                                     col("pickup_datetime"),
                                     col("dropoff_datetime"),
                                     col("fare_amount"),
                                     col("total_amount"),
                                     col("trip_distance"),
                                     lit(dataset)),
                           256))

#Seleção final
df_curated = df_curated.select(
    "trip_id", "type_id", "calendar_id", "pickup_datetime", "dropoff_datetime",
    "trip_distance", "fare_amount", "total_amount", "year", "month"
)

#Escrita final
output_path = f"s3://gpe-ingestao/curated/{dataset}/"
print(f"Gravando dados em {output_path}")
df_curated.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
print("Escrita concluída com sucesso.")

job.commit()