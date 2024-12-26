import pandas as pd 
import boto3
import requests
import os
from io import StringIO


url = 'https://pokeapi.co/api/v2/pokemon?limit=1350'

key = os.environ['KEY']
key_destination = os.environ['KEY_DESTINATION']
bucket = os.environ['BUCKET']


s3_client = boto3.client('s3')

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    content = s3_client.get_object(Bucket = bucket, Key = 'bronze-layer/pokemon.csv')
    df = content["Body"].read().decode("utf-8")
    try:
        df = pd.read_csv(StringIO(df))
        print('Get Object Successful')
    except Exception as erro:
        print('Error getting the object:', erro)

    df['primary_type'] = None
    df.set_index('name', inplace = True)
    for i in df.index:
        dic = requests.get(df.loc[i, 'url']).json()
        df['primary_type'].loc[i] = dic['types'][0]['type']['name']
    df.drop(columns = 'url', inplace = True)
    buffer = StringIO()
    df.to_csv(buffer, index = False)
    conteudo = buffer.getvalue()
    try:
        s3_client.put_object(Bucket = bucket, Key = key_destination, Body = conteudo)
    except Exception as erro:
        print('Error: ', erro)