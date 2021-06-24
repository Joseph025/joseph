import sys
sys.path.append("/home/dc_dev/idms_v2/")
from src.utils import common
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,StructField,StructType

def data_clean(s):
    s = str(s)
    if common.data_is_NULL(s):
        return ""
    s = s.replace('"',"'")
    s = s.replace(',',"。")
    s = s.replace('\n',"")
    s = s.replace('\r',"")
    return s


def put(spark,data):
    single = []
    list_data=data.toPandas().values.tolist()
    for row in list_data:
        dicts = {
                 "name": data_clean(row[0]),
                 "describe": data_clean(row[1]),
                 "detail":data_clean(row[2])
             }
        single.append(dicts)
    return single

    # data.registerTempTable("tmp_wys3160")
    # spark.udf.register("data_clean",data_clean)
    # spark.sql("select data_clean(name) from tmp_wys3160").show()


    # rddrow=data.rdd.map(lambda row: dict(data_clean(row[0]), data_clean(row[1]), data_clean(row[2])))


    # data是dataframe
    # single=[]
    # for row in data.toLocalIterator():
    #     dicts = {
    #         "name": data_clean(row[0]),
    #         "describe": data_clean(row[1]),
    #         "detail":data_clean(row[2])
    #     }
    #     single.append(dicts)
    # return single

