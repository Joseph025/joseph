# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 10:31:27 2018

@author: z15186

@基于字符匹配的问题单类型提取
"""
import re
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext,DataFrameWriter
import sys
import datetime
from data_clean import *
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class idms_classifier():
	'''
	基于规则的字符串匹配，从而确定问题单的类型
	'''
	kw_leak = ['内存泄漏','内存泄露','泄漏','泄露','内存耗尽','资源泄漏','句柄泄露','未释放的','无法释放','资源释放','内存块增长','未删除','未被删除','没有释放','残留','表项残留','未释放','内存释放','未释放内存','没有释放内存','遗漏删除','内存残留','忘记释放','需要释放','出现残留','遗漏释放']
	kw_lock = ['死锁','进程互锁','锁死','锁住','互锁','线程死锁','锁异常','信号量锁死','信号量死锁','任务调度死锁','lipc死锁','任务死锁','锁保护','持锁状态','写锁','释放锁','嵌套加锁','重复加锁','自旋锁','(?<!指纹|密码)解锁(?!失败)','rcu锁','互斥保护','读锁','嵌套使用','保护锁','锁竞争','spin_lock','自锁','锁中断','信号量互锁','并发访问','中断加锁','拿锁顺序','临界资源','同时访问','加锁互等','mutex互斥锁','mutex锁','并发读','加解锁操','读写锁','写者优先','解锁函数','遗漏解锁','锁嵌套','加锁顺序','加锁保护','重复解锁']
	kw_pointer = ['野指针','访问野指针','释放后的内存','释放后的','非法指针','误释放','已释放指针','重入','访问释放的地址','已经释放','重复释放','物理释放','失效的指针']
	kw_out = ['内存越界','写越界','越界访问','数组越界','访问越界']
		
	def __init__(self,text=None,pattern=None):
		self.pattern = pattern
		self.text = text
	
	def __cpattern(self,kwlist):
		return "|".join(iter(kwlist))
	
	def leak_classifier(self,text,pattern=None):
		if pattern is None:
			pattern = self.__cpattern(self.kw_leak)
		res = re.search(pattern,str(text))
		if res is None:
			return []
		else:
			return ['资源泄漏']
	
	def lock_classifier(self,text,category,pattern=None):
		if pattern is None:
			pattern = self.__cpattern(self.kw_lock)
		res = re.search(pattern,str(text))
		if res is None:
			return category
		else:
			return category+['锁']
	
	def out_classifier(self,text,category,pattern=None):
		if pattern is None:
			pattern = self.__cpattern(self.kw_out)
		res = re.search(pattern,str(text))
		if res is None:
			return category
		else:
			return category+['内存越界']
	
	def pointer_classifier(self,text,category,pattern=None):
		if pattern is None:
			pattern = self.__cpattern(self.kw_pointer)
		res = re.search(pattern,str(text))
		if res is None:
			if re.search('减翻',text) is not None and (re.search('访问',text) is not None or re.search('越界',text) is not None or re.search('释放',text) is not None):
				return category+['野指针']
			else:
				return category
		else:
			return category+['野指针']

def get1stclass(categorysList):
    category=set()
    for categorys in categorysList:
        category = category | set(categorys[1])
    if len(category) == 0:
        return '其它'
    else:
        return ",".join(sorted(category))

def idms_predict(data2):
	#"预测类型"
	ocidms = idms_classifier()

	return data2.map(lambda row : [row['defectid'],row['summery']+clean_detail(row['content'])])\
		.map(lambda row : [row[0],row[1],ocidms.leak_classifier(row[1])])\
		.map(lambda row : [row[0],row[1],ocidms.lock_classifier(row[1],row[2])])\
		.map(lambda row : [row[0],row[1],ocidms.out_classifier(row[1],row[2])])\
		.map(lambda row : [row[0],ocidms.pointer_classifier(row[1],row[2])])\
		.groupBy(lambda row:row[0])\
		.map(lambda row : [row[0],get1stclass(row[1]),datetime.datetime.now()])

def readDataFromHive(hiveContext,input_table):
	sql = "select defectid,summery,content\
		from " + input_table +"\
		where defecttype <> 'HardwareDefect'"
	return hiveContext.sql(sql).fillna('')

def saveToHive(hiveContext,rddData,out_table):
	hiveContext.sql("truncate table "+out_table)
	df = hiveContext.createDataFrame(rddData, ["defectid", "categories","loaded_timestamp"])
	DataFrameWriter(df).insertInto(out_table)
	

if __name__ == '__main__':
	out_table="quality_outbound_hive.outbound_defect_category"
	input_table="quality_outbound_hive.hive_ml_model_data_prepare"
	if len(sys.argv) == 2:
		input_table+=sys.argv[1]
		out_table+=sys.argv[1]
		logging.info("表输入为:"+input_table+"\t表输出为:"+out_table)
	else:
		logging.info("使用默认表输入表输出")
	#初始化hive context
	conf = SparkConf().setAppName('defect order classifier')
	sc = SparkContext(conf=conf)
	hiveContext = HiveContext(sc)
	data = readDataFromHive(hiveContext,input_table)
	if data.count() ==0:
		sc.stop()
		sys.exit()
	outRdd = idms_predict(data.rdd)
	saveToHive(hiveContext,outRdd,out_table)