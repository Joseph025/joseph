from pyspark import StorageLevel
from pyspark.sql import SparkSession,functions
from pyspark.sql.functions import concat_ws, split, when, substring,udf,expr
from pyspark import SparkConf,SparkContext
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import *
from pyspark.sql import Row
import numpy as np
import re
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.column import Column,_to_java_column
from pyspark.rdd import ignore_unicode_prefix


def saveToHive(spark,out_rdd,out_table):
    schema = StructType([StructField("serial_number_pre", StringType(), True),
                         StructField("pcb_number_pre", StringType(), True),
                         StructField("serial_number_tmp", StringType(), True),
                         StructField("chain", StringType(), True)])
    df = spark.createDataFrame(out_rdd,schema)
    DataFrameWriter(df).insertInto(out_table)


def doProcess(df_all):
    return df_all.rdd.map(lambda row:[row['serial_number_pre'], row["pcb_number_pre"], row["serial_number_tmp"], row['chain']])


def getResultData(spark,ser_data):
    ser_data.createOrReplaceTempView("tb_ser_two")
    df_ser_one = spark.sql("""select serial_number,pcb_number,
    (case when serial_number=pcb_number then pcb_number
    when serial_number!=pcb_number then concat_ws("/", serial_number, pcb_number) end) as chain
    from tb_ser_two""").withColumnRenamed("pcb_number", "pcb_number_pre").withColumnRenamed("serial_number", "serial_number_pre")

    while True:
        df_ser_one.createOrReplaceTempView("tb_ser_one")
        sql = """select serial_number_pre,nvl(pcb_number,pcb_number_pre) as pcb_number_pre,serial_number as serial_number_tmp,
                    (case when a.pcb_number_pre=b.serial_number and b.serial_number=b.pcb_number then chain
                    when b.serial_number is null and b.pcb_number is null then chain
                    when a.pcb_number_pre=b.serial_number and b.serial_number!=b.pcb_number
                    then concat_ws("/",chain,pcb_number) end) as chain from (select * from tb_ser_one) a
                    left join
                    (select * from tb_ser_two) b
                    on a.pcb_number_pre = b.serial_number"""
        df_ser_one = spark.sql(sql).distinct().filter("serial_number_tmp != ''").cache()
        # ①交集，将交集从ser_data删除，以便后续循环
        df_tmp = df_ser_one.join(ser_data, df_ser_one["serial_number_tmp"] == ser_data["serial_number"], "inner").select("serial_number", "pcb_number").distinct()
        # ser_data去除交集后的差集
        ser_data = ser_data.subtract(df_tmp)
        if df_tmp.count() == 0 or ser_data.count() == 0:
            break

    return df_ser_one


if __name__ == '__main__':
    # conf = SparkConf()
    # conf.set("spark.sql.warehouse.dir", "file:\\E:\\tmp\\hive2")
    # conf.set("spark.debug.maxToStringFields", 100)
    # spark = SparkSession.builder.appName("POC")\
    #     .config('spark.executor.extraJavaOptions', '-Dfile.encoding=utf-8') \
    #     .config('spark.driver.extraJavaOptions', '-Dfile.encoding=utf-8')\
    #     .enableHiveSupport().getOrCreate()
    spark = SparkSession.builder \
        .appName("poc") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.sql.warehouse.dir", "warehouse_dir") \
        .config("spark.sql.shuffle.partitions", "180") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("use partspoc")
    out_table = 'wufan.poc2'
    ser_data = spark.sql("SELECT serial_number,pcb_number from r_sn_pcb_relation_tmp") #r_sn_pcb_relation_tmp
    df_all = getResultData(spark,ser_data)
    df_all.show()
    # out_rdd = doProcess(df_all)
    # saveToHive(spark,out_rdd,out_table)
    spark.stop()