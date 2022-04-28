#!/usr/bin/env python
# coding: utf-8

# In[36]:

from pyspark import SparkFiles
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType,StructField,TimestampType,DateType,IntegerType

spark = SparkSession.        builder.        appName("pyspark-notebook").        master("spark://spark-master:7077").        config("spark.executor.extraClassPath", "/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar").        config("spark.driver.extraClassPath", "/root/airflowmount/spark/connectors/mssql-jdbc-9.4.1.jre8.jar").         config("spark.executor.memory",'8g') .        getOrCreate()
#spark = SparkSession.        builder.        appName("pyspark-notebook").        master("spark://spark-master:7077").        config("spark.executor.extraClassPath", "/opt/workspace/spark/connectors/mssql-jdbc-6.4.0.jre8.jar").        config("spark.driver.extraClassPath", "/opt/workspace/spark/connectors/mssql-jdbc-6.4.0.jre8.jar").         config("spark.executor.memory",'10g') .        getOrCreate()


spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/clfparser.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/faker.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/text_unidecode.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/six.py")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/random_user_agent.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/idna.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/charset_normalizer.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/urllib3.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/requests.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/certifi.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/colorama.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/tqdm.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/ua_parser.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/user_agents.zip")
spark.sparkContext.addPyFile("/root/airflowmount/spark/dependencies/country_list.zip")



# In[37]:


df_logs = spark.read.format('text').load('/root/airflowmount/storage/logs')
#df_logs = spark.read.format('text').load('/root/airflowmount/storage/logs/*.txt')


# In[38]:


from clfparser import CLFParser
from datetime import datetime

def get_ip(logRecord):
    clfDict=CLFParser.logDict(logRecord)
    return clfDict['h']

def get_useragent(logRecord):
    clfDict=CLFParser.logDict(logRecord)
    return clfDict['Useragent']

def get_username(logRecord):
    clfDict=CLFParser.logDict(logRecord)
    return clfDict['u']

def get_time(logRecord):
    clfDict=CLFParser.logDict(logRecord)
    if isinstance(clfDict['time'],datetime):
        time =  clfDict['time'].strftime("'%Y-%m-%d %H:%M:%S'").replace("'","")
    else:
        time = '1950-01-01 00:00:00'
    return time
#

get_ip_udf = udf(get_ip,StringType())
get_useragent_udf = udf(get_useragent,StringType())
get_username_udf = udf(get_username,StringType())
get_time_udf = udf(get_time,StringType())


# In[39]:


df_complete = df_logs                .withColumn('ipaddr',get_ip_udf(f.col('value')))                .withColumn('useragent',get_useragent_udf(f.col('value')))                .withColumn('username',get_username_udf(f.col('value')))                .withColumn('time',f.to_timestamp(get_time_udf(f.col('value'))))
df_complete.count()


# In[42]:


#remove where was not possible to parse the date assigning a specific datetime and then filtering out
df_complete_processed = df_complete.where("time != '1950-01-01 00:00:00'")


# In[43]:


df_complete_processed.count()


# In[45]:


df_countries = spark.read.format('text').load('/root/airflowmount/spark/assets/country_list.csv').collect()
country_list = [x[0] for x in df_countries]


# In[46]:


# Code for getting geolocations for ip - 15k requests per hour - 1 ip per request
# Since it's very slow on free plans, a dummy list was used to return the country's IP

import requests
from tqdm import tqdm
import time
from random import randrange

def get_geolocation_df(loaded_ips, dummy = True):
    if dummy == False:
        get_rows = map(lambda row: row.asDict(), loaded_ips.collect())
        iplist = [l['ipaddr'] for l in get_rows]
        response = []
        for i in tqdm(iplist):
            r = requests.get(f'https://api.freegeoip.app/json/{i}?apikey=6d86c060-7a44-11ec-b2aa-d3ef4e25c562').json()
            time.sleep(0.12)
            response.append(r)
        #payload = [l for l in get_rows]
        #response = requests.post("http://ip-api.com/batch", json=payload).json()
    else:
        get_rows = map(lambda row: row.asDict(), loaded_ips.collect())
        iplist = [l['ipaddr'] for l in get_rows]
        response = []
        for i in tqdm(iplist):
            response.append({'country_name':country_list[randrange(0,len(country_list))] , 'ip':i})
    return response


# In[47]:


#Get ips from the logs 
loaded_ips = df_complete.select(f.col('ipaddr'))

#Run API to get country information
geolocation_dict = get_geolocation_df(loaded_ips)

geoschema = StructType([       
        StructField('country_name', StringType(), True),
        StructField('ip', StringType(), True)
    ])

#Build Dataframe from returned dict
df_geo = spark.createDataFrame(data=geolocation_dict,schema=geoschema)


# In[48]:


#Get country ip addres on df
df_country_ip = df_complete.select('ipaddr','username').alias('c').join(df_geo.select('country_name','ip'),df_complete.ipaddr == df_geo.ip,'left')
df_country_ip.show()


# In[70]:


#Df complete with ipaddr and contry
df_with_country = df_complete.alias('dfc').join(df_country_ip.alias('dfcip'),f.col('dfc.ipaddr') == f.col('dfcip.ipaddr'), 'left').select('dfc.ipaddr','dfc.username','dfc.time','dfcip.country_name','dfc.useragent')
df_with_country.show(10)


# In[71]:


#Get device info from useragent
from user_agents import parse 

def get_os_family(useragent):
    return parse(useragent).os.family

def get_is_mobile(useragent):
    return int(parse(useragent).is_mobile)

def get_is_pc(useragent):
    return int(parse(useragent).is_pc)

def get_is_tablet(useragent):
    return int(parse(useragent).is_tablet)

get_os_family_udf = udf(get_os_family,StringType())
get_is_mobile_udf = udf(get_is_mobile,IntegerType())
get_is_pc_udf = udf(get_is_pc,IntegerType())
get_is_tablet_udf = udf(get_is_tablet,IntegerType())


# In[72]:


df_final = df_with_country.withColumn('device',get_os_family_udf('useragent'))                                  .withColumn('is_mobile',get_is_mobile_udf('useragent'))                                  .withColumn('is_pc',get_is_pc_udf('useragent'))                                  .withColumn('is_tablet',get_is_tablet_udf('useragent'))


# In[73]:


df_final.show()


# In[74]:


#Setup connection to staging db
server_name = "jdbc:sqlserver://host.docker.internal"
database_name = "Staging"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "Logs"
username = "sa"
password = "toptal"


# In[76]:


#Write to staging
df_final.drop('useragent')        .write        .format('jdbc')        .mode('overwrite')        .option("url", url)         .option("port", 1533)        .option("dbtable",table_name)         .option("user", username)         .option("password", password)         .save()

