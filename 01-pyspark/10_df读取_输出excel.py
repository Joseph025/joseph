
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import *

if __name__ == '__main__':
    conf = SparkConf()
    cluster_warehouse = 'warehouse_dir'
    # local_warehouse = 'file:\\C:\\Users\\wys3160\\software\\tmp\hive2'
    conf.set("spark.sql.warehouse.dir", cluster_warehouse)
    conf.set("spark.debug.maxToStringFields", 1000)
    conf.set("spark.sql.parquet.binaryAsString","true")
    spark = SparkSession.builder.appName("test") \
        .config('spark.executor.extraJavaOptions', '-Dfile.encoding=utf-8') \
        .config('spark.driver.extraJavaOptionsservice_contract_id', '-Dfile.encoding=utf-8') \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .enableHiveSupport().getOrCreate()

    # spark.sql("select * from wufan.order_config limit 3").toPandas().to_csv("/home/dc_dev/order.csv")


    # schema = StructType([StructField("Contract_Number", StringType(), True),
    #                      StructField("SN2", StringType(), True)])
    # spark.createDataFrame(pd.read_excel('/home/dc_dev/条码汇总.xlsx',engine='openpyxl',sheet_name="21年"),schema).write.saveAsTable("wufan.barcode_all",mode='append')
    #
    # 新代码
    # schema1=StructType([StructField("product_number",StringType(),True)])
    # spark.createDataFrame(pd.read_csv('/home/dc_dev/products.csv'),schema=schema1).write.saveAsTable("wufan.products",mode="overwrite")
    #
    # schema2=StructType([StructField("service_contract_number",StringType(),True)])
    # spark.createDataFrame(pd.read_csv('/home/dc_dev/service_contract.csv'),schema=schema2).write.saveAsTable("wufan.service_contract",mode="overwrite")
    #
    # schema3=StructType([StructField("service_contract_number_product_number_qty",StringType(),True)])
    # spark.createDataFrame(pd.read_csv('/home/dc_dev/relation.csv',error_bad_lines=False),schema=schema3).write.saveAsTable("wufan.relation", mode="overwrite")


    # a=spark.sql("""select * from wufan.relation""").toPandas()['service_contract_number_product_number_qty'].str.split("|")
    # spark.createDataFrame(a,['service_contract_number','products','qty']).write.saveAsTable("wufan.relation",mode="overwrite")