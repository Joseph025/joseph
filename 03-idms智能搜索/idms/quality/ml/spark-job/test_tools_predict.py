# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 11:39:32 2018

@author: f13702
"""

import os
import random
import re
import jieba
import jieba.posseg as psg
import logging
import datetime
import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext,DataFrameWriter
from data_clean import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

dic_words = dict([line.strip().split('\t') for line in open("zidian.dic", 'r', encoding='utf-8').readlines()])
#stop_words = [line.strip() for line in open("stopword.dic", 'r', encoding='utf-8').readlines()]+['\r','\r\n','\n','\u200d','\u3000']

#######字典预测
def zidian_predict(data):
    #处理含空格词语
    data = re.sub("soap\sui","SOAPUI",data,flags=re.I)
    data = re.sub("acunetix\sweb\svulnerability\sscanner","AWVS",data,flags=re.I)
    jieba.load_userdict("keyword.dic")
    predict=[]
    for t_word,t_c in psg.lcut(data):
        if str(t_word).lower() in dic_words:
            predict.append((t_word,dic_words.get(str(t_word).lower())))
        else:
            predict.append((t_word,'0'))
    return predict

#######规则第三版############################################
def guize_predict(words):
    
    #匹配ixia+数字,去掉lixia等形式  ixia\d*
    patternIxia = re.compile('ixia\d*')
    
    #匹配tc+数字,去掉mtcp等形式  tc\d*$|tcc$ tc之后以c或是数字结尾
    patternTc = re.compile('tc\d*$|TC\d*$|tcc$|tcleagle$')
    patternTc_sub = re.compile('tc$|TC$')  ####收到/打入+tc+数字是工具，其余的收到/打入+tc不是工具。
    
     #匹配数字+bps
    patternBps = re.compile('bps$|Bps$')
    
   #匹配smartbit、smatbit、smartbits+数字 ixia\d*
    patternSmartbit = re.compile('smartbit\d*$|smatbit\d*$|smartbits\d*$')
    
   #####匹配tester
    patternTester=re.compile('tester\w$')
    
     #####匹配smb
    patternSmb=re.compile('smb\w*$')
    
    
     #####匹配TTCN
    patternTTCN=re.compile('TTCN\d*$')
    
    #匹配非英文
    pattern = re.compile('[a-zA-Z]')
    ####匹配前后的特殊情况
    pattern_tc1 = re.compile('模式$|报文$|攻击$|关掉$|值$')
    pattern_tc2 = re.compile('打入$|收到$')
    pattern_bps1 = re.compile('是$|单位是$')
    pattern_bps2 = re.compile('\d*')
    pattern_bps3 = re.compile('\d*bps$')
    
    #匹配标点符号'[。：、”；、，/>#]'，但是目前没用
    #patternBiaodianfuhao = re.compile('[。：、“”；、，/>#？，\]\*,]')

    for i in range(0,len(words)):
        if i==0:
            if patternTc.match(words[i][0]):
                #####去除tc和ixia类前后是字母的情况
                if pattern.match(words[i+1][0]) or pattern_tc1.match(words[i+1][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('tc'))
       
            elif patternIxia.match(words[i][0]):
                if pattern.match(words[i+1][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('ixia'))
             
            elif patternBps.match(words[i][0]):
                if pattern_bps3.match(words[i][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('bps'))
        if i==len(words)-1:
            if patternTc.match(words[i][0]):
                #####去除tc和ixia类前后是字母的情况
                if pattern.match(words[i-1][0]) \
                or (patternTc_sub.match(words[i][0]) and pattern_tc2.match(words[i-1][0])):  
                    ####收到/打入+tc+数字是工具，其余的收到/打入+tc不是工具。
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('tc'))
       
            elif patternIxia.match(words[i][0]):
                if pattern.match(words[i-1][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('ixia'))
             
            elif patternBps.match(words[i][0]):
                if pattern_bps1.match(words[i-1][0]) or pattern_bps2.match(words[i-1][0]) \
                    or pattern_bps3.match(words[i][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('bps'))
        else:
            #rule4
            if patternTc.match(words[i][0]):
                
                #####去除tc和ixia类前后是字母的情况
                if (pattern.match(words[i-1][0]) and pattern.match(words[i+1][0])) \
                or pattern_tc1.match(words[i+1][0]) or \
                (patternTc_sub.match(words[i][0]) and pattern_tc2.match(words[i-1][0])):  ####收到/打入+tc+数字是工具，其余的收到/打入+tc不是工具。
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('tc'))
       
            elif patternIxia.match(words[i][0]):
                if (pattern.match(words[i-1][0]) and pattern.match(words[i+1][0]) ) or pattern.match(words[i-1][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('ixia'))
             
            elif patternBps.match(words[i][0]):
                if pattern_bps1.match(words[i-1][0]) or pattern_bps2.match(words[i-1][0]) \
                    or pattern_bps3.match(words[i][0]):
                    words[i] = (words[i][0],'0')
                else:
                    words[i] = (words[i][0],dic_words.get('bps'))

        if patternSmartbit.match(words[i][0]):
            words[i] = (words[i][0],dic_words.get('smartbits'))

        elif patternTester.match(words[i][0]):
            words[i] = (words[i][0],dic_words.get('tester'))

        elif patternSmb.match(words[i][0]):
            words[i] = (words[i][0],dic_words.get('smb'))

        elif patternTTCN.match(words[i][0]):
            words[i] = (words[i][0],dic_words.get('ttcn'))
    return words
                  

#######规则第三版############################################
def guize_predict_yingjian(words,defect_type,featurename):
    if defect_type !='HardwareDefect' and featurename != '硬件' and featurename !='Hardware':
        return words
    #匹配tc+数字,在硬件问题单中，不是工具
    patternTc = re.compile('tc\d*$|TC\d*$|tcc$|tcleagle$')
    pattern = re.compile('tc\D*$|TC\D*$')

    for i in range(0,len(words)):
        #####圈定tc类工具，找出tc+数字的，在硬件类型下不为工具。
        if patternTc.match(words[i][0]) and words[i][1]!='0':
            if not pattern.match(words[i][0]) :
                words[i] = (words[i][0],'0')
    return words

def final_predict(wordsList):
    test_tools=set()
    for words in wordsList:
        test_tools = test_tools | set([word[1] for word in words[1] if word[1]!='0'])
    #test_tools = [word[0] for word in words if word[1]==1]
    if len(test_tools) == 0:
        return ''
    else:
        return ",".join(test_tools)

def readDataFromHive(hiveContext,input_table):
    sql = "select defectid,summery,content,defecttype,featurename \
        from " + input_table +" \
        where nodecode = '1' and ( productlinename = '路由器产品线' or productlinename = '交换机产品线' )"
    return hiveContext.sql(sql).fillna('')

def doProcess(data):
    return data.map(lambda row : [row['defectid'],row['summery']+clean_detail_nrlc(row['content']),row['defecttype'],row['featurename']])\
        .map(lambda row : [row[0],zidian_predict(row[1]),row[2],row[3]])\
        .map(lambda row : [row[0],guize_predict(row[1]),row[2],row[3]])\
        .map(lambda row : [row[0],guize_predict_yingjian(row[1],row[2],row[3])])\
		.groupBy(lambda row:row[0])\
        .map(lambda row : [row[0],final_predict(row[1]),datetime.datetime.now()])
        
def saveToHive(hiveContext,rddData,out_table):
    hiveContext.sql("truncate table "+out_table)
    df = hiveContext.createDataFrame(rddData, ["defectid", "test_tools","loaded_timestamp"])
    DataFrameWriter(df).insertInto(out_table)
    logging.info("Test tool predict finished successed.")
        
if __name__ == '__main__':
    out_table="quality_outbound_hive.outbound_defect_test_tools"
    input_table="quality_outbound_hive.hive_ml_model_data_prepare"
    if len(sys.argv) == 2:
        input_table+=sys.argv[1]
        out_table+=sys.argv[1]
        logging.info("表输入为:"+input_table+"\t表输出为:"+out_table)
    else:
        logging.info("使用默认表输入表输出")
    #读取数据
    conf = SparkConf().setAppName('test tool predict')
    sc = SparkContext(conf=conf)
    hiveContext = HiveContext(sc)
    data = readDataFromHive(hiveContext,input_table)
    if data.count() ==0:
        sc.stop()
        sys.exit()
    outRdd = doProcess(data.rdd)
    saveToHive(hiveContext,outRdd,out_table)