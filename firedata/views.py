from django.conf import settings 
from django.http import JsonResponse
from rest_framework.decorators import api_view
from collections import defaultdict
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

