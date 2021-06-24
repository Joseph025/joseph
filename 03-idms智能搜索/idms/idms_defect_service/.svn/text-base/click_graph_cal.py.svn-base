import jieba
import pickle
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s')
stop_words = ['，', '。', '：', '；', '？', '【', '】', '、', '！', '-', ' ', '（', '）', '[', ']', '(', ')', ',', '.',\
              ';', '?', '!', ':']


def init_spark():
    return SparkSession.builder.appName("IDMS Click Graph Calculate").enableHiveSupport().getOrCreate()


def cal_click_graph(defect_no, query, stop_words):
    click_graph = {}
    click_graph[defect_no] = click_graph.get(defect_no, 0) + 1
    if 'summary' in query:
        query = query.split('""summary"": ""')[1].split('""')[0]
        words = list(set([word for word in jieba.lcut(query) if word not in stop_words]))
        for word in words:
            w_query_no = (word, defect_no)
            click_graph[w_query_no] = click_graph.get(w_query_no, 0) + 1
    return click_graph


def reduce_click_graph(graph1, graph2):
    for key, val in graph2.items():
        graph1[key] = graph1.get(key, 0) + val
    return graph1


if __name__ == '__main__':
    se = init_spark()
    logging.info("inited spark session")
    sql = "select log_query, defectno from quality_outbound_hive.idms_search_log_recall \
        where log_date >= add_months(current_date, -3) AND click_i IS NOT NULL"
    df = se.sql(sql)
    logging.info("Loaded data from spark, total rows count is :" + str(df.count()))
    final_graph = df.rdd.map(lambda row: cal_click_graph(row['defectno'], row['log_query'], stop_words))\
        .reduce(lambda graph1, graph2: reduce_click_graph(graph1, graph2))
    logging.info("Calculate dic finished")
    with open('SortAlgorithm/click_graph', 'wb') as out_put:
        pickle.dump(final_graph, out_put)
    se.stop()
    logging.info("IDMS Click Graph Calculate Completed")
