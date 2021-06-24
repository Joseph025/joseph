import sys
sys.path.append(r"/home/dc_dev/idms_v2/src")
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType
from pyspark.conf import SparkConf
from service import kdd
from src.service import extraction


def readDataFromHive(spark,input_table):
    sql="select defectNo as name,summary as describe,content as detail from "+input_table+" limit 2"
    return spark.sql(sql).fillna('')


if __name__ == '__main__':
    input_table="wufan.es_idms_defect_v4"
    conf = SparkConf()
    cluster_warehouse = 'warehouse_dir'
    local_warehouse = 'file:\\C:\\Users\\wys3160\\software\\tmp\hive2'
    conf.set("spark.sql.warehouse.dir", cluster_warehouse)
    conf.set("spark.debug.maxToStringFields", 1000)
    # conf.set("spark.sql.parquet.binaryAsString","true")
    # config("spark.sql.shuffle.partitions","180")
    spark = SparkSession.builder.appName("test") \
        .config('spark.executor.extraJavaOptions', '-Dfile.encoding=utf-8') \
        .config('spark.driver.extraJavaOptionsservice_contract_id', '-Dfile.encoding=utf-8')\
        .config("spark.sql.shuffle.partitions","220")\
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .enableHiveSupport().getOrCreate()
    # spark=SparkSession.builder.appName("knowledge").enableHiveSupport().getOrCreate()
    data = readDataFromHive(spark,input_table)
    if data.count() == 0:
        spark.stop()
        sys.exit()

    contentList=extraction.put(data)
    list_kdd=kdd.discovery_from_delhtmllabel(contentList)
    schema = StructType([StructField("name", StringType(), True),
                         StructField("describe", StringType(), True),
                         StructField("detail", StringType(), True),
                         StructField("describekey",StringType(),True),
                         StructField("detailkey",StringType(),True),
                         StructField("searchkey",StringType(),True)])
    # write.saveAsTable("quality_carbon_new.es_idms_defect_v4_output", mode="overwrite")
    spark.createDataFrame(list_kdd,schema).show()
    spark.stop()



