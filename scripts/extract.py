from airflow import DAG
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import json

def extract_transform_data(moedas, **kwargs):
    # Inicializar a sessão do Spark
    spark = SparkSession.builder \
        .appName("Extract and Transform Data") \
        .getOrCreate()
    
    # Extrair dados da API
    api_url = f"https://economia.awesomeapi.com.br/json/last/{moedas}"
    response = requests.get(api_url)
    data = response.json()
    data_json = json.dumps(data)


# Testando a função
extract_transform_data('USD-BRL')
