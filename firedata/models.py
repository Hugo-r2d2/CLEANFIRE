from django.db import models

class IncidenciaQueimada(models.Model):
    id = models.CharField(max_length=50, primary_key=True)
    data_hora = models.CharField(max_length=100)
    estado = models.CharField(max_length=100)
    municipio = models.CharField(max_length=100)
    bioma = models.CharField(max_length=100)
    dias_sem_chuva = models.IntegerField()
    precipita = models.FloatField(null=True, blank=True)
    latitude = models.FloatField()
    longitude = models.FloatField()
    frp = models.FloatField()

    def __str__(self):
        return f'{self.estado} - {self.municipio} - {self.data_hora}'