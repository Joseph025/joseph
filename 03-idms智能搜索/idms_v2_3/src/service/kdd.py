import os
import copy
from src.utils import common
from pandas.core.frame import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import when,_collect_list_doc,split,concat_ws
import pandas as pd

from pyspark.sql.types import StringType,StructField,StructType,ArrayType,DataType
import re
from bs4 import BeautifulSoup
from src.service import wordSegment
from src.app import cache
from src.dao import issueconstruction


g_word_segment_black_files  = os.path.join(os.path.dirname(os.getcwd()), \
                                          'src', \
                                          'cfg', \
                                          "blackList.txt")

g_align_label = ['iConfig',
    'iFeatureV7B70',
    'iCustomer',
    'iPDTPL',
    'iPDTSwitch',
    'iPDTSwitchSeries',
    'iPDTSwitchSpecs',
	'iPDTSwitch',
    'iPDTPL',
    'iPDTRouter',
    'iPDTRouterSeries',
    'iPDTRouterSpecs',
    'iPDTNew_network',
    'iPDTNew_networkSeries',
    'iPDTNew_networkSpecs',
    'iPDTIP_Wlan',
    'iPDTIP_WlanSeries',
    'iPDTIP_WlanSpecs',
    'iPDTH3C_Soft',
    'iPDTH3C_SoftSeries',
    'iPDTH3C_SoftSpecs',
    'iPDTIP_Security',
    'iPDTIP_SecuritySeries',
    'iPDTIP_SecuritySpecs',
    'iPDTServer',
    'iPDTServerSeries',
    'iPDTServerSpecs',
    'iPDTStorage',
    'iPDTStorageSeries',
    'iPDTStorageSpecs',
    'iPDTFusion_Architecture',
    'iPDTFusion_ArchitectureSeries',
    'iPDTFusion_ArchitectureSpecs',
    'iPDTH3Cloud',
    'iPDTH3CloudSeries',
    'iPDTH3CloudSpecs',
    'iPDTBig_Data',
    'iPDTBig_DataSeries',
    'iPDTBig_DataSpecs',
    'iPDTIoT',
    'iPDTIoTSeries',
    'iPDTIoTSpecs',
    'iPDTBig_Security',
    'iPDTBig_SecuritySeries',
    'iPDTBig_SecuritySpecs',
    'iPDTLTE',
    'iPDTLTESeries',
    'iPDTLTESpecs',
    'iPDTTransmission',
    'iPDTTransmissionSeries',
    'iPDTTransmissionSpecs',
    'iPDTAngle',
    'iPDTAngleSeries',
    'iPDTAngleSpecs',
    'iPDTCabling',
    'iPDTCablingSeries',
    'iPDTCablingSpecs',
    'iPDTH3CloudOS',
    'iPDTH3CloudOSSeries',
    'iPDTH3CloudOSSpecs',
    'iPDTIntelligence_Center',
    'iPDTIntelligence_CenterSeries',
    'iPDTIntelligence_CenterSpecs',
    'iPDTIntelligence_Home',
    'iPDTIntelligence_HomeSeries',
    'iPDTIntelligence_HomeSpecs',
    'iPDTMini',
    'iPDTMiniSeries',
    'iPDTMiniSpecs',
    'iPDTStandard_Network',
    'iPDTStandard_NetworkSeries',
    'iPDTStandard_NetworkSpecs',
    'iPDTH3C',
    'iPDTSpecsProperty_Switchs',
    'iPDTSpecsProperty_Router',
    'iPDTSpecsProperty_NewNetwork',
    'iPDTSpecsProperty_IPWlan',
    'iPDTSpecsProperty_H3CSoft',
    'iPDTSpecsProperty_IPSecurity',
    'iPDTSpecsProperty_Server',
    'iPDTSpecsProperty_Storage',
    'iPDTSpecsProperty_FusionArchitecture',
    'iPDTSpecsProperty_H3Cloud',
    'iPDTSpecsProperty_BigData',
    'iPDTSpecsProperty_IoT',
    'iPDTSpecsProperty_BigSecurity',
    'iPDTSpecsProperty_LTE',
    'iPDTSpecsProperty_Transmission',
    'iVerCMWV7Branch',
    'iVerCMWV7B23',
    'iVerCMWV7B35',
    'iVerCMWV7B45',
    'iVerCMWV7B70',
    'iVerH3C',
    'iWordDict',
    'iRealation']


'''
key:alias,value:name
'''
g_primary_key_align_dict = {}


def load_eneity_align_dict(labelList):
    # label是给定的一个个label
    for label in labelList:
        # 得到label数据
        data = cache.get(label)

        for k in data.keys():
            name  = k
            alias = data[k]

            if common.data_is_NULL(alias):
                continue
            if isinstance(alias,list):
                for a in alias:
                    g_primary_key_align_dict.update({a:name})
            else:
                g_primary_key_align_dict.update({alias:name})

        return g_primary_key_align_dict


def word_primary_key_align(words, isExpend=True):
    global g_primary_key_align_dict

    if common.data_is_NULL(g_primary_key_align_dict):
        g_primary_key_align_dict=load_eneity_align_dict(g_align_label)

    if common.data_is_NULL(g_primary_key_align_dict):
        return words

    retWords = []
    for w in words:
        if w in g_primary_key_align_dict.keys():
            w1 = g_primary_key_align_dict[w]
            if w1 != w:
                if isExpend:
                    retWords.append(w1)
                else:
                    w = w1
        retWords.append(w)
    return retWords


def merge(row):
    s_dict = dict()
    s_dict[row['name']] = row['searchkey']
    # keys是一个个neme
    for keys in s_dict.keys():
        # 通过key得到值
        list_w = []
        for words in s_dict[keys].split(","):
            words = str(words)
            words = words.replace("[", "")
            words = words.replace("]", "")
            # words = words.replace(" ", "")
            words = words.lstrip()
            words = words.rstrip()
            if words not in list_w:
                list_w.append(words)
            else:
                continue
        w_dict_2 = {
            "name": keys,
            "searchkey": list_w
        }
        return w_dict_2


# ⑤关键字
def search(df1,spark):
    # 还没加关键字
    df_search=df1.withColumn("searchkey",f.concat_ws(',',"describekey","detailkey"))
    rdd_search=df_search.rdd.map(lambda row: merge(row))
    schema=StructType([StructField("name",StringType(),True),
                       StructField("searchkey",StringType(),True)])
    df=spark.createDataFrame(rdd_search,schema)
    df.registerTempTable("tb_searchkey")
    df1.registerTempTable("tb_df1")
    df_end=spark.sql("select a.*,b.searchkey from tb_df1 a inner join (select name,searchkey from tb_searchkey) b on a.name=b.name")
    df_end.write.saveAsTable("quality_carbon_new.es_idms_defect_v4_output_2",mode="append")
    spark.stop()


# 白名单筛选rdd
def match_rule(whiteList,row):
    w_dict=dict()
    w_dict[row['name']]=row['detailkey']
    # keys是一个个neme
    for keys in w_dict.keys():
        # 通过key得到值
        list_w=[]
        for words in w_dict[keys].split(","):
            words=str(words)
            words=words.replace("[","")
            words=words.replace("]","")
            words=words.lstrip()
            words=words.rstrip()
            # words=words.replace(" ","")
            list_w.append(words)
        w_dict_2 = {
            "name":keys,
            "detailkey":list(set(list_w).intersection(set(whiteList)))
        }
        return w_dict_2


def white(spark,df_black):
    retList = []
    whiteListFile = issueconstruction.g_word_segment_white_file
    if common.data_is_NULL(whiteListFile):
        return df_black
    whiteList = common.read_file_lines_to_list(whiteListFile)
    if common.data_is_NULL(whiteList):
        return []

    # 将白名单whiteList转换成一行一列的dataframe
    # schema=StructType([StructField("white",StringType(),True)])
    # df_white=spark.createDataFrame(DataFrame(whiteList), schema) # ["white"]
    # df_all=df_white.withColumn("name",f.lit("白名单"))
    # df_all.registerTempTable("tmp_all")
    # df_whiteList=spark.sql("select concat_ws(',',collect_set(white)) as detailkey from tmp_all group by name")
    #
    # # 将白名单独立出来
    # rule_white=df_whiteList.rdd.map(lambda row: row).reduce(sum)
    # rule_white=spark.sparkContext.broadcast(rule_white)

    rdd=df_black.rdd.map(lambda row: match_rule(whiteList, row))
    schema=StructType([StructField("name",StringType(),True),
                       StructField("detailkey",StringType(),True)])
    df_detailkey=spark.createDataFrame(rdd,schema)
    return df_detailkey


# ④白名单筛选,
def words_white_list_process_new(spark,df_black):
    df_detailkey=white(spark,df_black)
    df_detailkey.registerTempTable("tb_detailkey")
    df_black.registerTempTable("tb_black")
    df1=spark.sql("select a.name,a.describe,a.detail,a.describekey,b.detailkey from tb_black a"
              " inner join (select name,detailkey from tb_detailkey) b on a.name=b.name")
    search(df1,spark)


# 黑名单的udf
def black(words):
    blackListFile = issueconstruction.g_word_segment_black_files
    if blackListFile is None:
        return not blackListFile
    else:
        retList = []
        for d in words:
            if d in blackListFile:
                continue
            if d in retList:
                continue
            retList.append(d)
    return retList


# ③黑名单过滤
def words_black_list_process_new(spark,df_primary):
    blackListFile = issueconstruction.g_word_segment_black_files

    if common.data_is_NULL(blackListFile):
        return df_primary

    blackList = common.read_file_lines_to_list(blackListFile)

    if common.data_is_NULL(blackList):
        return df_primary

    df_primary.registerTempTable("tb_black_content")
    spark.udf.register("black",black)
    df_black=spark.sql("select name,describe,detail,trim(black(describekey)) as describekey,trim(detailkey) as detailkey from  tb_black_content")

    # 白名单筛选
    words_white_list_process_new(spark,df_black)




# ②df是经过html清洗后的数据
def discovery_from_wordseg_new(spark,df):
    # 通过dataframe分词
    df.registerTempTable("tb_jieba_content")
    spark.udf.register("run",wordSegment.run)
    # 分词
    df_jieba_content=spark.sql("select name,describe,detail,run(describe) as describekey"
                               ",run(detail) as detailkey from tb_jieba_content")
    # 索引
    df_jieba_content.registerTempTable("tb_primary_content")
    spark.udf.register("word_primary_key_align",word_primary_key_align)
    df_primary=spark.sql("select name,describe,detail,word_primary_key_align(describekey) as describekey,"
                         "word_primary_key_align(detailkey) as detailkey from tb_primary_content")

    words_black_list_process_new(spark,df_primary)


# 将方法设置成udf
def clean_content(s):
    s=str(s)
    destStr = ''
    src_soup = BeautifulSoup(s, 'html5lib')
    if src_soup is not None:
        # get_text得到html内容
        src_soup_text = src_soup.get_text()
        if src_soup_text:
            destStr = src_soup_text.replace('\n', '')
            destStr = destStr.replace('\t', '')
            destStr = re.sub('\\s+', ' ', destStr)
    return destStr


# ①用rdd新换
def discovery_from_delhtmllabel_new(spark,df_content):
    df_content.registerTempTable("tb_content")
    spark.udf.register("clean_content",clean_content)
    df=spark.sql("select name,describe,case when detail is not null or detail != null then clean_content(detail) else  detail end detail from tb_content")
    discovery_from_wordseg_new(spark,df)
