import datetime as dt
from datetime import timedelta
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from elasticsearch import helpers



#find the highest revenue generating products
#find the top 5 highest selling products in each region 
#find month over month growth comparison for 2022 and 2023 sales
#for each category which month had the highest sales

#Function to import table from postgres server
def queryPostgresql():
    #Establishing a connection to postgres server
    conn_string= "dbname='Orders_database' host='localhost' user='postgres' password='postgres'"
    conn= db.connect(conn_string)

    #reading data from postgres database into DataFrame
    df = pd.read_sql("select * from new_orders", conn)
    df.to_csv('postgresqldata.csv')

#Function to insert csv file into Elasticsearch
def insertElasticsearch():
    es= Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme':'http'}], request_timeout= 200)
    #read the csv file into a DataFrame
    df = pd.read_csv("postgresqldata.csv")
    json_str = df.to_json(orient='records')

    json_records = json.loads(json_str)

    index_name = 'new_orders'
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400)
    action_list = []
    for row in json_records:
        record ={
            '_op_type': 'index',
            '_index': index_name,
            '_source': row
        }
        action_list.append(record)
    helpers.bulk(es, action_list)

    
#Creating DAG
#specifying DAG arguments for your DAG.
default_args ={
    'owner': 'Evans Boateng',
    'start_date': dt.datetime(2024, 12, 6),
    'retries':1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    dag_id ='New_project_dag',
    default_args=default_args,
    schedule='@once'
) as dag:
    
    #task_1 to retrieve data using the queryPostgresql function
    getData = PythonOperator(
        task_id = 'QueryPostgresql',
        python_callable=queryPostgresql
    )

    # task_2 that inserts the retrieved data into elasticsearch
    insertData = PythonOperator(
        task_id = 'InsertDataElasticsearch',
        python_callable= insertElasticsearch
    )

    #task_3 that prints out "...Done inserting" when the insertion is done
    printing = BashOperator(
        task_id = 'Done',
        bash_command= '...done inserting csv file into Elasticsearch'
    )
    
    #making connections between tasks
    getData >> insertData >> printing





    
