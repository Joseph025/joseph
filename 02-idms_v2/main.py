import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from src.service import kdd
from src.service import extraction


def readDataFromHive(spark,input_table):
    sql="""select OWNER,PMAnalysis,Rname,Vname,adminAdvice,approverComments,att_file_num1,
            att_file_num3,att_img_num1,att_img_num3,baseline,category,categoryStr,
            causeAnalysis,creationdate,currentNode,currentPerson,cut_words,defectModifier,
            defectNo,defect_ODCSeverity,developerComments,issueProcessor,lastProcessed,
            lastupdateTimestamp,lengthofstay,nodeCode,nodeName,operation_type,productLineName,
            productName,refresh_timestamp,solution,status,submitBy,submitDate,
            suspendReason,testReport,testTool,testToolStr,testerComments,defectid as name,summary as describe,content as detail from """+input_table+""
    return spark.sql(sql).fillna('')


if __name__ == '__main__':
    input_table="wufan.es_idms_defect_v4"
    conf = SparkConf()
    cluster_warehouse = 'warehouse_dir'
    local_warehouse = 'file:\\C:\\Users\\wys3160\\software\\tmp\hive2'
    conf.set("spark.sql.warehouse.dir", cluster_warehouse)
    conf.set("spark.debug.maxToStringFields", 1000)
    # conf.set("spark.sql.parquet.binaryAsString","true")
    spark = SparkSession.builder.appName("test") \
        .config('spark.executor.extraJavaOptions', '-Dfile.encoding=utf-8') \
        .config('spark.driver.extraJavaOptionsservice_contract_id', '-Dfile.encoding=utf-8')\
        .config("spark.sql.shuffle.partitions","220")\
        .config("spark.sql.parquet.enableVectorizedReader", "false").config("spark.sql.broadcastTimeout","6000") \
        .enableHiveSupport().getOrCreate()
    # spark=SparkSession.builder.appName("knowledge").enableHiveSupport().getOrCreate()
    data = readDataFromHive(spark,input_table)
    if data.count() == 0:
        spark.stop()
        sys.exit()
    df_content=extraction.put(spark,data)
    list_kdd=kdd.discovery_from_delhtmllabel_new(spark,df_content)
    spark.stop()



