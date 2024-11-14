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

# Listar dados do DynamoDB    
@api_view(['GET'])
def listar_dados_dynamoDB(request):
    dynamodb = conectar_dynamodb()
    table = dynamodb.Table('AMQ')

    try: 
        response = table.scan()
        items = response.get('Items', [])

        # Ajusta o formato dos itens para o serializer
        dados_convertidos = []
        for item in items:
            try: 
                # Substituir vírgulas por pontos e converter para float nos campos necessários
                latitude = float(item.get('Latitude', '0').replace(',', '.'))
                longitude = float(item.get('Longitude', '0').replace(',', '.'))
                frp = float(item.get('FRP', '0').replace(',', '.')) if item.get('FRP') else None
                precipita = float(item.get('Precipita', '0').replace(',', '.')) if item.get('Precipita') else None
            except (ValueError, AttributeError):
                latitude = None
                longitude = None
                frp = None
                precipita = None

            try:
                # Converte DiasSemChuva para int 
                dias_sem_chuva = int(float(item.get('DiasSemChuva', '0')))
            except (ValueError, TypeError):
                dias_sem_chuva = None

            dados_convertidos.append({
                'ID': item.get('ID'),
                'Estado': item.get('Estado'),
                'Municipio': item.get('Municipio'),
                'DataHora': item.get('DataHora'),
                'Bioma': item.get('Bioma'),
                'Latitude': latitude,
                'Longitude': longitude,
                'FRP': frp,
                'Precipita': precipita,
                'DiasSemChuva': dias_sem_chuva              
            })
        # Agrupamento de Dados
        agrupamento_por_municipio = defaultdict(int)
        agrupamento_por_estado = defaultdict(int)

        for dado in dados_convertidos:
            estado = dado['Estado']
            municipio = dado['Municipio']

            # Incrementar contadores 
            agrupamento_por_municipio[(estado, municipio)] += 1
            agrupamento_por_estado[(estado)] += 1
        
        # Formatar resultados de agrupamento 
        agrupamento_municipios = [
            {
                'Estado': estado,
                'Municipio': municipio,
                'Incidencias': incidencias
            }
            for (estado, municipio), incidencias in agrupamento_por_municipio.items()
        ]

        agrupamento_estados = [
            {
                'Estado': estado,
                'Incidencias': incidencias
            }
            for estado, incidencias in agrupamento_por_estado.items()
        ]

        # Resposta final com dados processados
        resposta = {
            'dados': dados_convertidos,
            'agrupamento_por_municipio': agrupamento_municipios,
            'agrupamento_por_estado': agrupamento_estados,
        }

        return JsonResponse(resposta, safe=False)
    
    except Exception as e: 
        return JsonResponse({'status': 'Erro', 'mensagem': str(e)}, status=500)