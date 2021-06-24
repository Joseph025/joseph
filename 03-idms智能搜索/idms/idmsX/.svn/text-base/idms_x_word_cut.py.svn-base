import logging
import jieba
import datetime
import re
from pyspark.sql import SparkSession

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def word_cut(sentence):
    return ",".join([word for word in jieba.lcut(sentence) if re.match('[\da-zA-Z\u4e00-\u9fa5]',word) is not None])

if __name__ == '__main__':
    out_table = "quality_outbound_hive.idms_x_word_cut"
    se = SparkSession.builder.appName("IDMS X-Process Word Cut").enableHiveSupport().getOrCreate()
    df = se.sql("select distinct defectid,summery from quality_outbound_hive.hive_ml_model_data_prepare")
    df2rdd = df.rdd.map(lambda row : [row['defectid'],word_cut(row['summery']),datetime.datetime.now()])
    se.sql("truncate table "+out_table)
    se.createDataFrame(df2rdd, ["defectid", "words","loaded_timestamp"]).write.insertInto(out_table)
    se.stop()
