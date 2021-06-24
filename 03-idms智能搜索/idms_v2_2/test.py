from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as f
from pyspark.sql.types import StructField,StringType,StructType,IntegerType
import numpy as np

def data_clean(s):
    s = str(s)
    s = s.replace('"',"'")
    s = s.replace(',',"。")
    s = s.replace('\n',"")
    s = s.replace('\r',"")
    return s


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


    list=(["张三,丰",27,"男"],
          ["张无\n忌",15,"男"])
    schema=StructType([StructField("name",StringType(),True),
                       StructField("age",IntegerType(),True),
                       StructField("sex",StringType(),True)])
    df=spark.createDataFrame(list,schema)
    df.registerTempTable("wufan")

    spark.udf.register("data_clean",data_clean)
    spark.sql("select data_clean(name) from wufan").show()

    # 必须是pandas才能用这个
    np_list=np.array(df.toPandas()).tolist()
    print(np.array(np_list))


    # spark.sql("""select  OWNER,
    #                 PMAnalysis,
    #                 Rname,
    #                 Vname,
    #                 adminAdvice,
    #                 approverComments,
    #                 att_file_num1,
    #                 att_file_num3,
    #                 att_img_num1,
    #                 att_img_num3,
    #                 baseline,
    #                 category,
    #                 categoryStr,
    #                 causeAnalysis,
    #                 content,
    #                 creationdate,
    #                 currentNode,
    #                 currentPerson,
    #                 cut_words,
    #                 defectID,
    #                 defectModifier,
    #                 defectNo,
    #                 defect_ODCSeverity,
    #                 developerComments,
    #                 issueProcessor,
    #                 lastProcessed,
    #                 lastupdateTimestamp,
    #                 lengthofstay,
    #                 nodeCode,
    #                 nodeName,
    #                 operation_type,
    #                 productLineName,
    #                 productName,
    #                 refresh_timestamp,
    #                 solution,
    #                 status,
    #                 submitBy,
    #                 submitDate,
    #                 summary,
    #                 suspendReason,
    #                 testReport,
    #                 testTool,
    #                 testToolStr,
    #                 testerComments from quality_carbon_new.es_idms_defect_v4""").write.saveAsTable("wufan.es_idms_defect_v4", mode='overwrite')
    spark.stop()