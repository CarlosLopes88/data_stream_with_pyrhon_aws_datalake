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
DATABASE_NAME = os.getenv('GLUE_DATABASE_NAME')
ORIGIN_BUCKET_NAME = os.getenv('S3_ORIGIN_BUCKET_NAME')
CRAWLER_NAME = os.getenv('GLUE_CRAWLER_NAME')
ROLE_NAME = os.getenv('GLUE_ROLE_NAME')

# Verifica se as credenciais foram carregadas corretamente
if not all([ACCESS_KEY, SECRET_KEY, REGION]):
    raise ValueError("Alguma(s) credencial(ais) não foi(foram) encontrada(s).")


# Inicializa o cliente Glue
glue_client = boto3.client('glue', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)

#cria um banco de dados no AWS Glue
def criar_database(database_name):
    try:
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        print(f"Banco de dados '{database_name}' criado com sucesso!")
    except Exception as e:
        print(f"Erro ao criar o banco de dados '{database_name}': {e}")

def criar_crawler(crawler_name, bucket_name, role_name, database_name):
    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role=role_name,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': f's3://{bucket_name}',
                        'Exclusions': []
                    },
                ]
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            Configuration=json.dumps({
                'Version': 1.0,
                'Grouping': {
                    'TableGroupingPolicy': 'CombineCompatibleSchemas'
                }
            })
        )
        print(f"Crawler '{crawler_name}' criado com sucesso!")
    except Exception as e:
        print(f"Erro ao criar o crawler '{crawler_name}': {e}")

def executar_crawler(crawler_name):
    try:
        response = glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' iniciado com sucesso!")
    except Exception as e:
        print(f"Erro ao iniciar o crawler '{crawler_name}': {e}")

if __name__ == "__main__":
    criar_database(DATABASE_NAME)
    time.sleep(30)
    criar_crawler(CRAWLER_NAME, ORIGIN_BUCKET_NAME, ROLE_NAME, DATABASE_NAME)
    time.sleep(30)
    executar_crawler(CRAWLER_NAME)