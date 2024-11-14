from rest_framework import serializers
from .models import IncidenciaQueimada

class IncidenciaQueimadaSerializer(serializers.ModelSerializer):
    id = serializers.CharField(source='ID')
    data_hora = serializers.DateTimeField(source='DataHora')
    estado = serializers.CharField(source='Estado')
    municipio = serializers.CharField(source='Municipio')
    bioma = serializers.CharField(source='Bioma')
    latitude = serializers.FloatField(source='Latitude')
    longitude = serializers.FloatField(source='Longitude')
    frp = serializers.FloatField(source='FRP')
    precipita = serializers.FloatField(source='Precipita')
    dias_sem_chuva = serializers.IntegerField(source='DiasSemChuva')

    class Meta: 
        model = IncidenciaQueimada
        fields = '__all__'