from src.utils import common


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
    data.registerTempTable("clean")
    spark.udf.register("data_clean", data_clean)
    df=spark.sql("select data_clean(name) name,data_clean(describe) describe,data_clean(detail) detail from clean")
    return df

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

    # single = []
    # list_data=data.toPandas().values.tolist()
    # for row in list_data:
    #     dicts = {
    #              "name": data_clean(row[0]),
    #              "describe": data_clean(row[1]),
    #              "detail":data_clean(row[2])
    #          }
    #     single.append(dicts)
    # return single

