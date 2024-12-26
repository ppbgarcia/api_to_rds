import pandas as pd 
import boto3
import sqlalchemy
from os import getenv
from io import StringIO

url = getenv('URL')
user = getenv('USER')
pw = getenv('PW')
port = getenv('PORT')
bucket = getenv('BUCKET')

s3_client = boto3.client('s3')
try:
    df_pokemon = s3_client.get_object(Bucket = bucket, Key = 'silver-layer/pokemon.csv')
    df_pokemon = df_pokemon["Body"].read().decode("utf-8")
    df_pokemon = pd.read_csv(StringIO(df_pokemon))
    print('Get Object Successful')
except Exception as erro:
    print('Error getting the object:', erro)

df_gold = pd.DataFrame(df_pokemon['primary_type'].value_counts())

engine = sqlalchemy.create_engine(f"mysql+pymysql://{user}:{pw}@{url}:{port}/DEV")
df_gold.to_sql('pokemon_stats', con = engine, if_exists = 'replace')



