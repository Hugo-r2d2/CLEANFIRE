from django.urls import path 
from . import views

urlpatterns = [
    path('api/inserir-dados/', views.processar_csv_e_inserir_dados, name='inserir-dados'),
    path('api/queimadas/', views.listar_dados_dynamodb, name='listar_dados_dynamodb'),
]
