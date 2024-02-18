import boto3
import os
import json
from random import uniform
import time
from datetime import datetime
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtém as credenciais de acesso da AWS do arquivo .env
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')
STREAM_NAME = os.getenv('STREAM_NAME')

# Verifica se as credenciais foram carregadas corretamente
if not all([ACCESS_KEY, SECRET_KEY, REGION]):
    raise ValueError("Alguma(s) credencial(ais) não foi(foram) encontrada(s).")

# Cria um cliente para o serviço de Kinesis
try:
    cliente_kinesis = boto3.client('kinesis', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)
    print("Cliente kinesis inicializado com sucesso!")
except Exception as e:
    print("Erro ao inicializar o cliente kinesis:", e)

# Cria um gerador de dados para o Kinesis
id = 0
while True:
    try:
        dados = uniform(0.7, 1.0)
        id += 1
        registros = {'id_temp': str(id), 'data': str(dados), 'tipo': 'powerfactor', 'timestamp': str(datetime.now())}
        cliente_kinesis.put_record(StreamName=STREAM_NAME, Data=json.dumps(registros), PartitionKey='02')
        print(registros)
        time.sleep(10)
    except Exception as e:
        print("Ocorreu um erro:", e)