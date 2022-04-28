#!/usr/bin/env python
# coding: utf-8

# In[26]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession. builder.appName("pyspark-notebook").master("spark://spark-master:7077")\
. config("spark.executor.extraClassPath", "/root/airflow/spark/connectors/mssql-jdbc-9.4.1.jre8.jar")\
.config("spark.driver.extraClassPath", "/root/airflow/spark/connectors/mssql-jdbc-9.4.1.jre8.jar")\
    .getOrCreate()

spark.sparkContext.addPyFile("/root/airflow/spark/dependencies/clfparser.zip")
spark.sparkContext.addPyFile("/root/airflow/spark/dependencies/clfparser-0.3-py3.10.egg-info.zip")

# In[10]:

from clfparser import CLFParser 

server_name = "jdbc:sqlserver://host.docker.internal"
database_name = "B2BPlatform"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "customers"
username = "sa"
password = "toptal" # Please specify password here


# In[12]:


df = spark\
    .read \
    .format("jdbc") \
    .option("url", url) \
    .option("port", 1533)\
    .option("dbtable",f'(Select TOP 2 * from {table_name}) as customers_limited') \
    .option("user", username) \
    .option("password", password) \
    .load()


# In[27]:


#df.select(f.col('full_name')).show(30)


# In[45]:


df.coalesce(1).select('full_name').write.mode('overwrite').format('text').option("header", "false").save('/opt/workspace/storage/teste_pelo_airflow/')

df.select('full_name').write\
    .mode('append')\
    .format("jdbc") \
    .option("url", url) \
    .option("port", 1533)\
    .option("dbtable",'fromSparkAirflow') \
    .option("user", username) \
    .option("password", password) \
    .save()


df = spark\
    .read \
    .format("text") \
    .load('/opt/workspace/storage/teste_pelo_airflow/')
# In[49]:


'''spark_df = spark.createDataFrame(
    [
        (1, "Mark", "Brown"), 
        (2, "Tom", "Anderson"), 
        (3, "Joshua", "Peterson")
    ], 
    ('id', 'firstName', 'lastName')
)'''


# In[46]:


'''df_ = spark.read.format('text').load('./teste.log')'''

