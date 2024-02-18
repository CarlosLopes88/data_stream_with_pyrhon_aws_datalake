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
ROLE_NAME = os.getenv('GLUE_ROLE_NAME')

# Verifica se as credenciais foram carregadas corretamente
if not all([ACCESS_KEY, SECRET_KEY, REGION]):
    raise ValueError("Alguma(s) credencial(ais) não foi(foram) encontrada(s).")

# Inicializa o cliente Glue
glue_client = boto3.client('glue', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)

def criar_role_para_glue(role_name):
    # Inicializa o cliente IAM
    iam = boto3.client('iam')

    # Define a política de acesso para o Glue
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Cria a role para o Glue
    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        print("Role criada com sucesso!")
    except Exception as e:
        print("Erro ao criar a role:", e)

    # Anexa a política AdministratorAccess à role
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AdministratorAccess'
        )
        print("Política anexada com sucesso!")
    except Exception as e:
        print("Erro ao anexar a política:", e)

if __name__ == "__main__":
    criar_role_para_glue(ROLE_NAME)