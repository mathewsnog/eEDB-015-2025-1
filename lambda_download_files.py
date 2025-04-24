import os
import requests
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from botocore.exceptions import NoCredentialsError, ClientError

S3_BUCKET = "gpe-ingestao"
S3_PREFIX = "raw/"

URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_tripdata_{date}.parquet"

s3_client = boto3.client("s3")

def gerar_datas(inicio, fim):
    datas = []
    atual = inicio
    while atual <= fim:
        datas.append(atual.strftime('%Y-%m'))
        atual += relativedelta(months=1)
    return datas

def download_file(url):
    file_name = url.split('/')[-1]
    local_path = f"/tmp/{file_name}"

    try:
        print(f"Baixando {url}")
        response = requests.get(url, stream=True)

        if response.status_code == 404:
            print(f"Arquivo não encontrado: {url}")
            return None

        response.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): 
                if chunk:
                    f.write(chunk)

        print(f"Salvo em {local_path}")
        return local_path

    except requests.HTTPError as e:
        print(f"Erro ao baixar {url}: {e}")
        return None

def upload_to_s3(file_path, dataset, ano):
    file_name = os.path.basename(file_path)
    s3_key = f"{S3_PREFIX}{dataset}/{ano}/{file_name}"

    try:
        print(f"Enviando para s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print("Upload concluído")
    except (FileNotFoundError, NoCredentialsError, ClientError) as e:
        print(f"Erro no upload S3: {e}")

def lambda_handler(event, context):
    dataset = event.get("dataset")
    ano = int(event.get("ano"))

    # Define os anos válidos
    if dataset == "fhvhv" and ano < 2020:
        return {"statusCode": 400, "body": f"{dataset} só disponível a partir de 2020"}

    data_inicio = datetime(ano, 1, 1)
    data_fim = datetime(ano, 12, 1)

    datas = gerar_datas(data_inicio, data_fim)

    print(f"Processando {dataset} para o ano {ano}")

    for date in datas:
        url = URL_TEMPLATE.format(dataset=dataset, date=date)
        file_path = download_file(url)

        if file_path:
            upload_to_s3(file_path, dataset, ano)
            os.remove(file_path)
            print(f"Removido {file_path}")

    return {
        "statusCode": 200,
        "body": f"Processo concluído para {dataset} {ano}"
    }