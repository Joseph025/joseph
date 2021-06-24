import jieba
import time
import logging
from pyspark.sql import SparkSession
import pickle
from math import log, log10
import numpy as np
from collections import Counter
import pyltr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s')
stop_words = ['，', '。', '：', '；', '？', '【', '】', '、', '！', '-', ' ', '（', '）', '[', ']', '(', ')', ',', '.',
              ';', '?', '!', ':']


def trans_date(days):
    if days <= 30:
        return [1, 0, 0, 0]
    elif days <= 90:
        return [0, 1, 0, 0]
    elif days <= 180:
        return [0, 0, 1, 0]
    else:
        return [0, 0, 0, 1]


def data_clean(rdd_data):
    rdd_data = rdd_data.asDict(True)
    rdd_data.es_score = float(rdd_data.es_score)

    rdd_data['time_features'] = trans_date(rdd_data['time_features'])
    rdd_data['f1'] = 1 if rdd_data.currentNode >= '3' else 0
    rdd_data['att_file_num1'] = 0. if rdd_data['att_file_num1'] is None else float(rdd_data['att_file_num1'])
    rdd_data['att_img_num1'] = 0. if rdd_data['att_img_num1'] is None else float(rdd_data['att_img_num1'])
    rdd_data['att_file_num3'] = 0. if rdd_data['att_file_num3'] is None else float(rdd_data['att_file_num3'])
    rdd_data['att_img_num3'] = 0. if rdd_data['att_img_num3'] is None else float(rdd_data['att_img_num3'])

    if 'summery' in rdd_data.log_query:
        rdd_data.log_query = rdd_data.log_query.strip().split('""summary"": ""')[1].split('""')[0]
    else:
        rdd_data.log_query = ''

    rdd_data['click_i'] = 0 if rdd_data.click_i is None else 1
    rdd_data['summery'] = dict(Counter([w for w in jieba.lcut(rdd_data.summery) if w not in stop_words]))

    return rdd_data


def count_corpus(group_rows):
    p_idf = {}
    p_c = {}

    doc_lens = []
    for doc in group_rows:
        doc_lens.append(sum(doc.values()))
        for token in doc:
            p_idf[token] = p_idf.get(token, 0) + 1
            p_c[token] = p_c.get(token, 0) + doc[token]

    corpus = {
        'tokens_N': sum(doc_lens),
        'N': len(doc_lens),
        'p_idf': p_idf,
        'p_c': p_c
    }
    return corpus


def feature_engine(query_tokens, res_tokens, corpus, k1=1.5, b=0.75, lamb=0.1, mu=2000, delta=0.7):
    query_tokens_dict = dict(Counter(query_tokens))
    scores = []
    n = corpus.N
    tokens_n = corpus.tokens_N
    p_idf = corpus.p_idf

    doc_len = sum(res_tokens.values())
    c_single = res_tokens

    score_query_tf = 0
    score_query_idf = 0
    score_query_tfidf = 0
    score_query_bm25 = 0
    score_query_lmir_jm = 0
    score_query_lmir_dir = 0
    score_query_lmir_abs = 0
    score_doc_token_max_tf = max(c_single.values()) / doc_len
    score_doc_token_min_tf = min(c_single.values()) / doc_len

    score_query_token_max_tf = 0
    score_query_token_min_tf = 0.5
    score_query_token_max_idf = 0
    score_query_token_min_idf = 0.5
    score_query_token_max_tfidf = 0
    score_query_token_min_tfidf = 0.5
    score_doc_len = doc_len / (tokens_n / n)

    for token in query_tokens_dict:
        corpus_token_frequency = corpus.p_c.get(token, 0)  ####corpus 语料层面的
        if token in c_single:
            doc_token_frequency = c_single[token]

            token_tf = doc_token_frequency / max(doc_len, 1)  ###max(doc_len,1) 为了防止异常情况doc_len=0
            score_query_token_max_tf = max(score_query_token_max_tf, token_tf)
            score_query_token_min_tf = min(score_query_token_min_tf, token_tf)
            score_query_tf += token_tf * query_tokens_dict[token]

            token_idf = log(n / p_idf[token]) / n
            score_query_token_max_idf = max(score_query_token_max_idf, token_idf)
            score_query_token_min_idf = min(score_query_token_min_idf, token_idf)
            score_query_idf += token_idf * query_tokens_dict[token]

            token_tfidf = token_tf * token_idf
            score_query_tfidf += token_tfidf * query_tokens_dict[token]
            score_query_token_max_tfidf = max(score_query_token_max_tfidf, token_tfidf)
            score_query_token_min_tfidf = min(score_query_token_min_tfidf, token_tfidf)

            score_query_bm25 += query_tokens_dict[token] * token_idf * doc_token_frequency * (k1 + 1) / (
                            doc_token_frequency + k1 * (1 - corpus.b + corpus.b * doc_len * tokens_n / n))
        else:
            token_tf = 0
            doc_token_frequency = 0

        score_query_lmir_jm += ((1 - lamb) * token_tf + lamb * corpus_token_frequency / tokens_n) * \
            query_tokens_dict[token]
        score_query_lmir_dir += ((doc_token_frequency + mu * corpus_token_frequency / tokens_n) / (
            doc_len + mu)) * query_tokens_dict[token]
        score_query_lmir_abs += (max(doc_token_frequency - delta, 0) / max(doc_len, 1) + delta * len(
            c_single) / max(doc_len, 1) * (corpus_token_frequency / tokens_n)) * query_tokens_dict[token]

    scores = [log(score_doc_len + 1),
        score_doc_token_max_tf,  # *10
        score_doc_token_min_tf,  # *10
        score_query_token_max_tf,  # *10
        score_query_token_min_tf,  # *10 需要去除异常点0.5
        score_query_token_max_idf,  # *100
        score_query_token_min_idf,  # *100 需要去除异常点0.5
        score_query_token_max_tfidf,  # *1000
        score_query_token_min_tfidf,  # *1000 需要去除异常点0.5
        log(score_query_tf + 1),
        log(score_query_idf + 1),  # *10   暂定
        score_query_tfidf,  # *100
        # score_query_bm25,            # *1000
        log(score_query_lmir_jm + 1),
        log(score_query_lmir_dir + 1),
        log(score_query_lmir_abs + 1)]
    return scores


def obtain_features(rdd_data, click_graph):

    original_val = rdd_data[0]
    corpus_val = rdd_data[1]
    query_list = list(set([i for i in jieba.lcut(original_val.log_query) if i not in stop_words])) + \
        [original_val.defectno]

    original_val['click_features'] = [log(click_graph.get(query_list[-1], 0) + 1),
                                  np.mean([click_graph.get((i, query_list[-1]), 0) for i in query_list[:-1]])]

    start_time = time.time()
    sim_features = feature_engine(jieba.lcut(original_val.log_query), original_val.summery, corpus_val)
    original_val['features'] = sim_features + \
        [log10(original_val['click_features'][0] + 1), log10(original_val['click_features'][1] + 1)] + \
        [original_val['time_features']] + [original_val['f1']] + [original_val['att_file_num1']] + \
        [original_val['att_img_num1']] + [original_val['att_file_num3']] + [original_val['att_img_num3']] + \
        [log10(original_val['es_score'] + 1)]
    last_rdd = [[original_val['log_id']], [original_val['features']], [original_val['click_i']]]
    logging.info('process feature use time:' + str(time.time() - start_time))
    return last_rdd


def reduce_features(features1, features2):
    features1[0] = features1[0] + features2[0]
    features1[1] = features1[1] + features2[1]
    features1[2] = features1[2] + features2[2]
    return features1


if __name__ == '__main__':
    se = SparkSession.builder.appName("IDMS Click Graph Calculate").enableHiveSupport().getOrCreate()
    sql = "select a.log_id,a.log_query,a.es_score,a.defectid,a.defectno,a.summary,a.click_i, \
        b.currentNode,datediff(a.log_date, b.creationdate) as time_features, b.att_file_num1, b.att_file_num3 \
        b.att_img_num1, b.att_img_num3 from \
        quality_outbound_hive.idms_search_log_recall a left join quality_carbon.es_idms_defect_v2 b \
        on b.defectID = a.defectid "
    # read data from spark
    df = se.sql(sql)
    logging.info("get data from spark, rows number is:" + str(df.count()))

    # df data clean
    df = df.replace('', '000000000000', 'defectno').fillna('0', 'es_score')
    # load click graph
    with open('SortAlgorithm/click_graph', 'rb') as graph_input:
        click_graphs = pickle.load(graph_input)

    df_rdd = df.rdd.map(lambda row: (row['log_id'], data_clean(row)))
    corpus_rdd = df_rdd.map(lambda row: (row[0], [row[1]['summery']])).reduceByKey(lambda row1, row2: row1 + row2)\
        .map(lambda row: (row[0], count_corpus(row[1])))
    res = df_rdd.leftOuterJoin(corpus_rdd).map(lambda row: obtain_features(row[1], click_graphs))\
        .reduce(reduce_features)

    q_id = np.array(res[0])
    x = np.array(res[1])
    y = np.array(res[2])

    ###模型训练及其测试
    t1 = time.time()
    metric = pyltr.metrics.NDCG(k=10)

    model = pyltr.models.LambdaMART(
        metric=metric,
        n_estimators=1000,
        learning_rate=0.02,
        max_features=0.5,
        query_subsample=0.5,
        max_leaf_nodes=10,
        min_samples_leaf=64,
        verbose=1,
    )

    model.fit(x, y, q_id)
    logging.info('model train use time:' + str(time.time() - t1))
    with open('SortAlgorithm/model1', 'wb') as out_put:
        pickle.dump(model, out_put)
    logging.info("Finished model training")
