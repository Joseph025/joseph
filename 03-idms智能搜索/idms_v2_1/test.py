from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StructField,StringType,StructType,IntegerType
if __name__ == '__main__':
    input_table="quality_carbon_new.es_idms_defect_v4"
    conf = SparkConf()
    cluster_warehouse = 'warehouse_dir'
    local_warehouse = 'file:\\C:\\Users\\wys3160\\software\\tmp\hive2'
    conf.set("spark.sql.warehouse.dir", cluster_warehouse)
    conf.set("spark.debug.maxToStringFields", 1000)
    # conf.set("spark.sql.parquet.binaryAsString","true")
    spark = SparkSession.builder.appName("test") \
        .config('spark.executor.extraJavaOptions', '-Dfile.encoding=utf-8') \
        .config('spark.driver.extraJavaOptionsservice_contract_id', '-Dfile.encoding=utf-8') \
       .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .enableHiveSupport().getOrCreate()


    # list=(["joseph",27,"男"])
    # schema=StructType([StructField("NAME",StringType(),True),
    #                    StructField("AGE",IntegerType(),True),
    #                    StructField("SEX",StringType(),True)])
    # spark.createDataFrame(list,schema).show()

    spark.sql("""select  OWNER,
                    PMAnalysis,
                    Rname,
                    Vname,
                    adminAdvice,
                    approverComments,
                    att_file_num1,
                    att_file_num3,
                    att_img_num1,
                    att_img_num3,
                    baseline,
                    category,
                    categoryStr,
                    causeAnalysis,
                    content,
                    creationdate,
                    currentNode,
                    currentPerson,
                    cut_words,
                    defectID,
                    defectModifier,
                    defectNo,
                    defect_ODCSeverity,
                    developerComments,
                    issueProcessor,
                    lastProcessed,
                    lastupdateTimestamp,
                    lengthofstay,
                    nodeCode,
                    nodeName,
                    operation_type,
                    productLineName,
                    productName,
                    refresh_timestamp,
                    solution,
                    status,
                    submitBy,
                    submitDate,
                    summary,
                    suspendReason,
                    testReport,
                    testTool,
                    testToolStr,
                    testerComments from quality_carbon_new.es_idms_defect_v4""").write.saveAsTable("wufan.es_idms_defect_v4", mode='overwrite')
    spark.stop()