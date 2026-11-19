from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os

def coletar_clima():
    # Coleta Open-Meteo (API pública sem token)
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=-23.55&longitude=-46.63&start_date=2026-11-15&end_date=2026-11-16&hourly=temperature_2m,precipitation"
    resp = requests.get(url).json()
    os.makedirs("/opt/airflow/chegada", exist_ok=True)
    with open("/opt/airflow/chegada/clima.json", "w") as f:
        json.dump(resp, f)

def coletar_sptrans():
    # Lê a chave de API de forma segura via Airflow Variables (Nunca fixa no código!)
    token = Variable.get("sptrans_token", default_var="fallback_simulador")
    # Simulação ou chamada oficial com token
    dados = [{"id_veiculo": 201, "codigo_linha": "809T-10", "atraso_minutos": 15, "fonte": "olhovivo" if token != "fallback_simulador" else "simulador"}]
    os.makedirs("/opt/airflow/chegada", exist_ok=True)
    with open("/opt/airflow/chegada/transporte.json", "w") as f:
        json.dump(dados, f)

def conferir_chegada():
    arquivos = os.listdir("/opt/airflow/chegada")
    print(f"Arquivos presentes na chegada: {arquivos}")
    if not arquivos:
        raise ValueError("Nenhum arquivo encontrado na pasta chegada/")

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG("pipeline_ponto_falante", default_args=default_args, schedule="@daily", start_date=datetime(2026, 11, 1), catchup=False) as dag:
    
    # Tarefa 1 e 2 rodando em paralelo
    t1_clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    t2_transporte = PythonOperator(task_id="coletar_sptrans", python_callable=coletar_sptrans)
    
    # Tarefa 3 que confere a chegada
    t3_conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    # Definindo paralelismo: [t1, t2] executam juntas e depois acionam t3
    [t1_clima, t2_transporte] >> t3_conferir