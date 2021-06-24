
# coding: utf-8

# In[4]:


import re
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import logging
import requests
import json

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

#搜索日志
search_file = "idms-search_20180607.csv"
#点击日志
#click_file = "C:\\Users\\z14612\\MyPY\\IDMS\\src\\idms-ticket_20180607.csv"

select_header = ["FuncType","Sql"]
search_data = pd.read_csv(search_file,sep='\^\|~',header=0,usecols=select_header,na_filter=False,                          encoding="UTF-8",engine='python')

#rest param init
url="http://10.63.21.17:9205/_search"
headers={"content-type":"application/json; charset=UTF-8"}
es_data = []

def call_es(row):
    d_body = eval(row[1].strip('"').replace('""','"'))
    d_body['_source'] = ['defectID']
    d_body = json.dumps(d_body)
    response = requests.post(url,headers=headers,data=d_body)
    if response.status_code == 200:
        r_data = response.json()
        for hits in r_data['hits']['hits']:
            es_data.append({'log_id' : row[0], 'log_query' : row[1], 'es_score' : hits['_score'],                            'defectid' : hits['_source']['defectID']})

#call es api to get candidate list
search_data.applymap(lambda row : call_es(row))

#build spark session
se = SparkSession.builder.appName("IDMS Log Info Recall").enableHiveSupport().getOrCreate()
df_search = se.createDataFrame(es_data)
#get all IDMS data
df_all = se.sql("select * from table quality_outbound_hive.hive_ml_model_data_prepare_v2").fillna('')

#join
final_data = df_search.join(df_all,df_search.defectID == df_all.defectid,'left_outer')
df_headers = "^\|~".join(final_data.columns)
final_data = final_data.rdd.map(lambda rows: "^\|~".join([str(row) for row in rows]))
se.createDataFrame(final_data,df_headers).write.csv("file:///opt/tmp_folder/do",quote='',header=True)

