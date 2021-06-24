# -*- coding: utf-8 -*-
import pyspark.sql as spark
import logging
import re

def count_attached_a_num(content):
    return len(re.compile('<a.*?</a>').findall(content))

def count_attached_img_num(content):
    return len(re.compile('<img .*?>').findall(content))

#build spark session
se = spark.SparkSession.builder.appName("IDMS Log Info Recall").enableHiveSupport().getOrCreate()

sql = "select richtextid,content,lastupdate_timestamp from quality_landing_hive.richtext_incr"

#df = se.sql(sql).orderBy("richtextid",spark.functions.desc("lastupdate_timestamp")).dropDuplicates(["richtextid"])
df = se.sql(sql)

dr2rdd = df.rdd.map(lambda row: [row['richtextid'],count_attached_a_num(row['content']),\
                        count_attached_img_num(row['content']),row['lastupdate_timestamp']])

schema = spark.types.StructType([spark.types.StructField("richtextid",spark.types.StringType(),True),\
           spark.types.StructField("attached_file_num",spark.types.IntegerType(),True),\
           spark.types.StructField("attached_img_num",spark.types.IntegerType(),True),\
           spark.types.StructField("lastupdate_timestamp",spark.types.TimestampType(),True)])

se.sql("truncate table quality_outbound_hive.richtext_attached_info")
se.sql("insert into quality_outbound_hive.richtext_attached_info select * from \
    quality_outbound_hive.tmp_richtext_attached_info")
se.sql("drop table quality_outbound_hive.tmp_richtext_attached_info")
se.createDataFrame(dr2rdd, schema).write.insertInto("quality_outbound_hive.richtext_attached_info")

se.stop()

