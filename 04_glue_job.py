import boto3
import json
import os
from dotenv import load_dotenv
import time

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtém as credenciais de acesso da AWS do arquivo .env
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')
JOB_NAME = os.getenv('GLUE_JOB_NAME')
ROLE_NAME = os.getenv('GLUE_ROLE_NAME')
DATABASE_NAME = os.getenv('GLUE_DATABASE_NAME')
SOURCE_TABLE_NAME= os.getenv('GLUE_SOURCE_TABLE_NAME')
SCRIPT_DESTINATION_BUCKET = os.getenv('S3_SCRIPT_DESTINATION_BUCKET')
SCRIPT_PATH= os.getenv('GLUE_SCRIPT_PATH')
# Conteúdo do script
SCRIPT_CONTENT = os.getenv('GLUE_SCRIPT_CONTENT')

# Verifica se as credenciais foram carregadas corretamente
if not all([ACCESS_KEY, SECRET_KEY, REGION]):
    raise ValueError("Alguma(s) credencial(ais) não foi(foram) encontrada(s).")

# Inicializa o cliente Glue
glue_client = boto3.client('glue', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)

def criar_etl_job(job_name, role_name, database_name, source_table_name, destination_bucket, script_path, script_content):
    try:
        # Salva o script no bucket S3 de destino
        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)
        s3_client.put_object(Bucket=destination_bucket, Key=script_path, Body=script_content)

        # URL do script no bucket S3
        script_location = f"s3://{destination_bucket}/{script_path}"

        # Cria o trabalho do Glue
        response = glue_client.create_job(
            Name=job_name,
            Role=role_name,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location
            },
            DefaultArguments={
                '--job-language': 'python',
                '--enable-continuous-cloudwatch-log': 'true'
            },
            GlueVersion='4.0',
            ExecutionProperty={
                'MaxConcurrentRuns': 1
            },
            Tags={
                'Name': job_name
            }
        )
        print(f"Job '{job_name}' criado com sucesso!")
    except Exception as e:
        print(f"Erro ao criar o job '{job_name}': {e}")

def executar_etl_job(job_name):
    try:
        # Inicia o job do Glue
        response = glue_client.start_job_run(JobName=job_name)
        print(f"Job '{job_name}' iniciado com sucesso!")
    except Exception as e:
        print(f"Erro ao iniciar o job '{job_name}': {e}")

if __name__ == "__main__":
    criar_etl_job(JOB_NAME, ROLE_NAME, DATABASE_NAME, SOURCE_TABLE_NAME, SCRIPT_DESTINATION_BUCKET, SCRIPT_PATH, SCRIPT_CONTENT)
    time.sleep(30)
    executar_etl_job(JOB_NAME)