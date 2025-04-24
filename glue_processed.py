import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

#Parâmetros
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATASET', 'ANO'])
dataset = args['DATASET']
ano_ref = int(args['ANO'])

#Inicialização do contexto Spark e Glue
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Coluna de tempo por dataset
if dataset == "yellow":
    pickup_col = "tpep_pickup_datetime"
elif dataset == "green":
    pickup_col = "lpep_pickup_datetime"
elif dataset == "fhvhv":
    pickup_col = "request_datetime"
else:
    pickup_col = "pickup_datetime"

#Leitura do parquet da camada Raw
path = f"s3://gpe-ingestao/raw/{dataset}/{ano_ref}/"
print(f"Lendo parquet de {dataset}/{ano_ref}...")
df = spark.read.parquet(path)

#Drop de colunas problemáticas e irrelevantes (inclui passenger_count)
cols_to_drop = [
    "airport_fee", "ehail_fee", "VendorID",
    "PUlocationID", "DOlocationID",
    "PULocationID", "DOLocationID",
    "SR_Flag", "passenger_count",
    "RatecodeID", "payment_type",
    "trip_type"
]

cols_presentes = [c for c in cols_to_drop if c in df.columns]
if cols_presentes:
    print(f"Dropando colunas: {cols_presentes}")
    df = df.drop(*cols_presentes)

#Cast seguro para colunas úteis no modelo
cast_map = {
    "pickup_locationid": "long",
    "dropoff_locationid": "long",
    "trip_distance": "double",
    "fare_amount": "double",
    "total_amount": "double"
}

for col_name, col_type in cast_map.items():
    if col_name in df.columns:
        try:
            print(f"Cast de {col_name} para {col_type}")
            df = df.withColumn(col_name, col(col_name).cast(col_type))
        except Exception as e:
            print(f"Erro ao castar {col_name}: {e}")

#Geração de colunas de partição
df = df.withColumn("ano", year(col(pickup_col)))
df = df.withColumn("mes", month(col(pickup_col)))

# Filtro por ano de referência
df = df.filter(col("ano") == ano_ref)

# Log de contagem
print(f"Registros após tratamento: {df.count()}")

#Escrita no S3 (Camada Processed)
output_path = f"s3://gpe-ingestao/processed/{dataset}/"
df.write.mode("overwrite").partitionBy("ano", "mes").parquet(output_path)
print(f"Escrita concluída com sucesso em {output_path}")

job.commit()