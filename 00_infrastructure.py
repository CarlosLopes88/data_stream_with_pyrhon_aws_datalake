import boto3
import os
from dotenv import load_dotenv
import time

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtém as credenciais de acesso da AWS do arquivo .env
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')
STREAM_NAME = os.getenv('STREAM_NAME')
ORIGIN_BUCKET_NAME = os.getenv('S3_ORIGIN_BUCKET_NAME')
DESTINY_BUCKET_NAME = os.getenv('S3_DESTINY_BUCKET_NAME')
FIREHOSE_NAME = os.getenv('FIREHOSE_NAME')
FIREHOSE_ROLE_ARN = os.getenv('FIREHOSE_ROLE_ARN')
SCRIPT_DESTINATION_BUCKET = os.getenv('S3_SCRIPT_DESTINATION_BUCKET')

# Verifica se as credenciais foram carregadas corretamente
if not all([ACCESS_KEY, SECRET_KEY, REGION]):
    raise ValueError("Alguma(s) credencial(ais) não foi(foram) encontrada(s).")

# Inicializa o cliente Kinesis
cliente_kinesis = boto3.client('kinesis', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)

# Inicializa o cliente S3
s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)

# Inicializa o cliente Firehose
firehose = boto3.client('firehose', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)

def criar_fluxo_kinesis(stream_name):
    try:
        # Verifica se o fluxo de dados já existe
        response = cliente_kinesis.list_streams()
        existing_streams = response.get('StreamNames', [])
        
        if stream_name not in existing_streams:
            # O fluxo de dados não existe, então cria o fluxo de dados
            response = cliente_kinesis.create_stream(StreamName=stream_name, ShardCount=1)
            print(f"Fluxo de dados '{stream_name}' criado com sucesso!")
        else:
            print(f"O fluxo de dados '{stream_name}' já existe.")
    except Exception as e:
        print("Erro ao criar o fluxo de dados do Kinesis:", e)

def criar_bucket(bucket_name):
    try:
        # Verifica se o bucket já existe
        response = s3.list_buckets()
        existing_buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name not in existing_buckets:
            # O bucket não existe, então cria o bucket
            s3.create_bucket(Bucket=bucket_name)
            print(f'Bucket "{bucket_name}" criado com sucesso!')
        else:
            print(f'O bucket "{bucket_name}" já existe.')
    except Exception as e:
        print(f'Ocorreu um erro ao criar o bucket: {e}')

def criar_fluxo_firehose(firehose_name, bucket_name, stream_name):
    try:
        # Verifica se o fluxo de entrega de dados já existe
        response = firehose.list_delivery_streams()
        existing_firehoses = response.get('DeliveryStreamNames', [])
        
        if firehose_name not in existing_firehoses:
            # O fluxo de entrega de dados não existe, então cria o fluxo
            response = firehose.create_delivery_stream(
                DeliveryStreamName=firehose_name,
                DeliveryStreamType='KinesisStreamAsSource',
                ExtendedS3DestinationConfiguration={
                    'RoleARN': FIREHOSE_ROLE_ARN,
                    'BucketARN': f'arn:aws:s3:::{bucket_name}',
                    'BufferingHints': {
                        'SizeInMBs': 5,
                        'IntervalInSeconds': 60
                    }
                },
                KinesisStreamSourceConfiguration={
                    'KinesisStreamARN': f'arn:aws:kinesis:{REGION}:740588470221:stream/{stream_name}',
                    'RoleARN': FIREHOSE_ROLE_ARN
                }
            )
            print(f"Fluxo de entrega de dados '{firehose_name}' criado com sucesso!")
        else:
            print(f"O fluxo de entrega de dados '{firehose_name}' já existe.")
    except Exception as e:
        print("Erro ao criar o fluxo de entrega de dados do Firehose:", e)

if __name__ == "__main__":
    criar_fluxo_kinesis(STREAM_NAME)
    time.sleep(10)
    criar_bucket(ORIGIN_BUCKET_NAME)
    time.sleep(10)
    criar_bucket(DESTINY_BUCKET_NAME)
    time.sleep(10)
    criar_bucket(SCRIPT_DESTINATION_BUCKET)
    time.sleep(10)
    criar_fluxo_firehose(FIREHOSE_NAME, ORIGIN_BUCKET_NAME, STREAM_NAME)