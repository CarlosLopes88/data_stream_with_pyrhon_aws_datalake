# Projeto de Dados Stream com Python e Datalake na Infraestrutura AWS

Este projeto consiste em uma aplicação Python para geração de dados aleatórios e envio para um bucket no Amazon S3, utilizando serviços como Kinesis, Firehose, Glue. Os dados são posteriormente processados por um job do AWS Glue e armazenados em um datalake em formato parquet.

## Pré-requisitos

- Conta na AWS com permissões para criar e gerenciar recursos como Kinesis, Firehose, Glue, IAM e S3.
- Python 3.x instalado na máquina local.
- Bibliotecas Python listadas no arquivo `requirements.txt`.
- Arquivo `.env` com as variáveis de ambiente configuradas conforme especificado.

## Estrutura de Diretórios

/  
├── 00_data_generator.py  
├── 01_infrastructure.py  
├── 02_glue_role.py  
├── 03_crawler.py  
├── 04_glue_job.py  
├── requirements.txt  
└── .env  


## Configuração para Execução

1. Configure as variáveis de ambiente no arquivo `.env` com as credenciais de acesso da AWS e outros parâmetros necessários.
2. Instale as dependências Python listadas no arquivo `requirements.txt` utilizando o pip: `pip install -r requirements.txt`.

## Execução Passo a Passo

1. Execute o script `00_data_generator.py` para gerar e enviar os dados aleatórios para o Amazon S3.
2. Execute o script `01_infrastructure.py` para criar as estruturas de fluxos e buckets no Amazon S3.
3. Execute o script `02_glue_role.py` para criar a role necessária para o Glue executar os fluxos.
4. Execute o script `03_crawler.py` para criar e executar o Crawler no Glue.
5. Execute o script `04_glue_job.py` para criar e executar o job do Glue para processar os dados.

Certifique-se de que todos os passos foram executados com sucesso e verifique os resultados nos recursos criados na AWS, como buckets S3, banco de dados no Glue e logs de execução dos jobs e crawlers.

## Tecnologias Aplicadas

- Amazon Kinesis
- AWS Firehose
- AWS Glue
- AWS IAM
- Amazon S3
- Python 3.x

ajuste