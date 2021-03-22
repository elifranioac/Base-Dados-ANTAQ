#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Preparação da DAG
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import, timedelta
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}
dag = DAG(
    dag_id='ANTAQ_ETL',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60)
)


# In[ ]:


# criar uma Task usanda para extração da tabela Acordo Bilateral
spark = SparkSession         .builder         .appName("ANTAQ.tb_ab")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela Atracacao
spark = SparkSession         .builder         .appName("ANTAQ.tb_atr")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela Carga
spark = SparkSession         .builder         .appName("ANTAQ.tb_carga")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela Carga_Conteinerizada
spark = SparkSession         .builder         .appName("ANTAQ.tb_carga_contz")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela Carga_Regiao
spark = SparkSession         .builder         .appName("ANTAQ.tb_carga_reg")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela TemposAtracacao
spark = SparkSession         .builder         .appName("ANTAQ.tb_temp_atr")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela atracacao_fato
spark = SparkSession         .builder         .appName("ANTAQ.atracacao_fato")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


# criar uma Task usanda para extração da tabela carga_fato
spark = SparkSession         .builder         .appName("ANTAQ.carga_fato")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

inicio = datetime.now()
print(inicio)


# In[ ]:


###Teste e Rascunhos


# In[ ]:


# 1. Imprime a data na saída padrão
task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)
# 2. Faz uma sleep de 5 segundos.
# Dando errado tente em no máximo 3 vezes
task2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)
# 3. Salve a data em um arquivo texto
task3 = BashOperator(
    task_id='save_date',
    bash_command='date > /tmp/date_output.txt',
    retries=3,
    dag=dag)


# In[ ]:


import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, functions

def processo_etl_spark():
    spark = SparkSession         .builder         .appName("ANTAQ")         .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

    inicio = datetime.now()
    print(inicio)
    # criação de data frame com extração de dados
    df = spark.read.format("jdbc")         .option("url", "jdbc:sqlserver://127.0.0.1:1433;databaseName=Teste")         .option("user", 'Teste')         .option("password", 'teste')         .option("numPartitions", 100)         .option("partitionColumn", "Id")         .option("lowerBound", 1)         .option("upperBound", 488777675)         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")         .option("dbtable", "(select Id, DataVencimento AS Vencimento, TipoCod AS CodigoTipoDocumento, cast(recsld as FLOAT) AS Saldo from DocumentoPagar          where TipoCod in ('200','17') and RecPag = 'A') T")         .load()
    # agrupamento de dados e agregação de valores
    group = df.select("CodigoTipoDocumento", "Vencimento", "Saldo")         .groupby(["CodigoTipoDocumento", "Vencimento"]).agg(functions.sum("Saldo").alias("Saldo"))
    # carregamento de dados para dentro da base mongo
    group.write.format("com.mongodb.spark.sql.DefaultSource")         .mode("overwrite")         .option("database", "Financeiro")         .option("collection", "Fact_DocumentoPagar")         .save()
    termino = datetime.now()
    print(termino)
    print(termino - inicio)

default_args = {
    'owner': 'jozimar',
    'start_date': datetime(2020, 11, 18),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('dag_teste_spark_documento_vencido_v01',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    task_elt_documento_pagar = PythonOperator(
        task_id='elt_documento_pagar_spark',
        python_callable=processo_etl_spark
    )


# In[ ]:


import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Elifranio',
    'start_date': datetime(2020, 11, 18),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('dag_teste_ANTAQ',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    task_elt_documento_pagar = BashOperator(
        task_id='elt_documento_pagar_spark',
        bash_command="python ./dags/sparkjob.py",
    )


# In[ ]:




