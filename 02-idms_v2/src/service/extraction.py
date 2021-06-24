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
    df=spark.sql("select OWNER,PMAnalysis,Rname,Vname,adminAdvice,approverComments,att_file_num1,att_file_num3,"
                 "att_img_num1,att_img_num3,baseline,category,categoryStr,causeAnalysis,creationdate,currentNode,"
                 "currentPerson,cut_words,defectModifier,defectNo,defect_ODCSeverity,developerComments,"
                 "issueProcessor,lastProcessed,lastupdateTimestamp,lengthofstay,nodeCode,nodeName,operation_type,"
                 "productLineName,productName,refresh_timestamp,solution,status,submitBy,submitDate,suspendReason,"
                 "testReport,testTool,testToolStr,testerComments,"
                 "data_clean(name) name,data_clean(describe) describe,data_clean(detail) detail from clean")
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

