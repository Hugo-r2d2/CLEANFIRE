from django.conf import settings 
from django.http import JsonResponse
from rest_framework.decorators import api_view
from collections import defaultdict
from .utils import importar_dados_csv, caminho_csv
import pandas as pd 
import boto3
import time

# Função que conecta o DynamoDB AWS
def conectar_dynamodb():
    return boto3.resource(
        'dynamodb',
        region_name = 'us-east-1',
        aws_access_key_id = settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
    )

# Função para inserir os dados no DynamoDB
def inserir_dados_no_dynamo(df):
    dynamodb = conectar_dynamodb()
    table = dynamodb.Table('AMQ') # Indica a tabela que irá receber os dados

    # Ordenar DataFrame pelo campo ID
    df = df.sort_values(by='ID')
    
    with table.batch_writer() as batch:
        for index, row in df.iterrows():
            item = {
                'ID' : str(row['ID']),
                'Estado' : row['Estado'],
                'Município' : row['Municipio'],
                'DataHora' : row['DataHora'].isformat(),
                'Bioma': row['Bioma'],
                'Latitude': str(row['Latidute']),
                'Longitude': str(row['Longitude']),
                'FRP': str(row['FRP']),
                'Precipita': str(row['Precipitacao']) if not pd.isnull(row['Precipitacao']) else None,
                'DiasSemChuva': str(row['DiaSemChuva']),
            }
            batch.put_item(Item=item)

            # Pausa para não ultrapassar o throughput
            if index % 25 == 0:
                time.sleep(2)

# Função para processar os dados no CSV e inserir no DynamoDB
def processar_csv_e_inserir_dados(request):
    try:
        # Importar os dados do CSV
        df = importar_dados_csv(caminho_csv)

        # Inserir dados no DynamoDB
        inserir_dados_no_dynamo(df)

        return JsonResponse({'status': 'Sucesso', 'mensagem': 'Dados inseridos com sucesso no DynamoDB'})
    except Exception as e: 
        return JsonResponse({'status': 'Erro', 'mensagem': str(e)})