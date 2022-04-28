#!/usr/bin/env python
# coding: utf-8

# In[16]:


from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType,StructField

spark = SparkSession.        builder.        appName("pyspark-notebook").        master("spark://spark-master:7077").        config("spark.executor.extraClassPath", "/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar").        config("spark.driver.extraClassPath", "/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar").        config("spark.executor.memory",'8g') .        getOrCreate()

spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/clfparser.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/faker.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/text_unidecode.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/six.py")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/random_user_agent.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/tqdm.zip")


# In[17]:


server_name = "jdbc:sqlserver://host.docker.internal"
database_name = "B2BPlatform"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "customers"
username = "sa"
password = "toptal" # Please specify password here


# In[19]:


df_customers = spark    .read     .format("jdbc")     .option("url", url)     .option("port", 1533)    .option("dbtable",table_name)     .option("user", username)     .option("password", password)     .load()


# In[4]:


#Fake Ip address 
from faker import Faker  
faker = Faker()  
ip_addr = faker.ipv4()  
print(ip_addr)


# In[5]:


#Collect customers so we can iterate over them
customers = [x[0] for x in df_customers.select("username").collect()]


# In[6]:


import random
def randompick(list_of):
    max = len(list_of)
    return customers[random.randrange(max)]


# In[7]:


from random import randrange
from datetime import timedelta, datetime

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

d1 = datetime.strptime('1/1/2021 00:00:00', '%m/%d/%Y %H:%M:%S')
d2 = datetime.strptime('12/31/2021 23:59:59', '%m/%d/%Y %H:%M:%S')

print(random_date(d1, d2))


# In[8]:


df_useragents = spark.read.format('text').load('/root/airflowmount/spark/assets/user_agents.txt')


# In[9]:


useragents = [x[0] for x in df_useragents.collect()]


# In[15]:


from tqdm import tqdm

#generate logs in 100k batches
for i in range(0,1):
    amountOfLogs = 100000
    loglist = []

    for i in tqdm(range(0,amountOfLogs)):
        logline = f"{faker.ipv4()} - {randompick(customers)} [{random_date(d1, d2).strftime('%d/%b/%Y:%H:%m:%S')} -0700] \"GET /favicon.ico HTTP/1.1\" 404 209 \"http://www.example.com/start.html\" \"{useragents[random.randrange(0,len(useragents))]}\""
        loglist.append(Row(logline))

    logSchema = StructType([       
        StructField('log', StringType(), True)
    ])

    df_logs = spark.createDataFrame(data=loglist, schema=logSchema)


# In[ ]:


df_logs.write.format('text').mode('overwrite').save('/root/airflowmount/storage/logs/')


# In[ ]:




