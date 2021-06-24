# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 17:55:56 2018

@author: w16847
"""



from math import log, log10
from collections import Counter
import pickle
import jieba
import numpy as np
import time


class FEATURES:
    def __init__(self, corpus, k1=1.5, b=0.75, lamb=0.1, mu=2000, delta=0.7):
        """Use language models to score query/document pairs.

        :param corpus:  需要线下离线完成部分计算，以便降低实时计算时间，要求corpus为[dict(),dict(),...,dict()],dict()为每个文档的词分布。
        :param doc_lens: 每个文档的词个数,list
        :param lamb: 
        :param mu: 
        :param delta: 
        """
        self.lamb = lamb
        self.mu = mu
        self.delta = delta
        self.k1 = k1
        self.b = b
        # Fetch all of the necessary quantities for the document language models.
        p_idf={}
        p_C={}

        doc_lens=[]
        for doc in corpus:
            doc_lens.append(sum(doc.values()))
            for token in doc:
                p_idf[token]=p_idf.get(token,0)+1
                p_C[token]=p_C.get(token,0)+doc[token]
                
        
        self.tokens_N = sum(doc_lens)
        self.N = len(corpus)
        self.c = corpus              ###每个文档的词个数，类型为字典
        self.doc_lens = doc_lens     ###每个文档的词个数
        self.p_idf = p_idf           ###每个文档的词idf
        self.p_C = p_C
    
    
    def feature_engine(self, query_tokens):
        query_tokens_dict = dict(Counter(query_tokens))
        scores=[]
        n=self.N
        tokens_n=self.tokens_N
        p_idf=self.p_idf
#        p_c=self.p_C
        for doc_idx in range(self.N):
            doc_len=self.doc_lens[doc_idx]
            c_single=self.c[doc_idx]
            
            
            score_query_tf = 0
            score_query_idf = 0
            score_query_tfidf = 0
            score_query_bm25 = 0
            score_query_lmir_jm = 0
            score_query_lmir_dir = 0
            score_query_lmir_abs = 0
            score_doc_token_max_tf = max(c_single.values())/doc_len
            score_doc_token_min_tf = min(c_single.values())/doc_len
            score_query_token_max_tf = 0
            score_query_token_min_tf = 0.5
            score_query_token_max_idf = 0
            score_query_token_min_idf = 0.5
            score_query_token_max_tfidf = 0
            score_query_token_min_tfidf = 0.5
            score_doc_len = doc_len/(tokens_n/n)

            for token in query_tokens_dict:
                corpus_token_frequency = self.p_C.get(token,0)                  ####corpus 语料层面的
                           
                if token in c_single:
                    doc_token_frequency = c_single[token]
                    
                    token_tf = doc_token_frequency/max(doc_len,1)     ###max(doc_len,1) 为了防止异常情况doc_len=0
                    score_query_token_max_tf = max(score_query_token_max_tf,token_tf)
                    score_query_token_min_tf = min(score_query_token_min_tf,token_tf)                    
                    score_query_tf += token_tf*query_tokens_dict[token]
                    
                    
                    token_idf = log(n/p_idf[token])/n                   
                    score_query_token_max_idf = max(score_query_token_max_idf,token_idf)
                    score_query_token_min_idf = min(score_query_token_min_idf,token_idf)
                    score_query_idf += token_idf*query_tokens_dict[token]
                    
                    token_tfidf = token_tf*token_idf
                    score_query_tfidf += token_tfidf*query_tokens_dict[token]
                    score_query_token_max_tfidf = max(score_query_token_max_tfidf,token_tfidf)
                    score_query_token_min_tfidf = min(score_query_token_min_tfidf,token_tfidf)
                                               
                    score_query_bm25 += query_tokens_dict[token]*token_idf*doc_token_frequency*(self.k1+1)/(doc_token_frequency+self.k1*(1-self.b+self.b*doc_len*tokens_n/n))
                else:
                    token_tf = 0
                    doc_token_frequency = 0
                    
                score_query_lmir_jm += ((1-self.lamb)*token_tf+self.lamb*corpus_token_frequency/tokens_n)*query_tokens_dict[token]
                score_query_lmir_dir += ((doc_token_frequency + self.mu * corpus_token_frequency/tokens_n) / (doc_len + self.mu))*query_tokens_dict[token]
                score_query_lmir_abs += (max(doc_token_frequency - self.delta, 0) / max(doc_len,1) + self.delta * len(c_single) / max(doc_len,1) * (corpus_token_frequency/tokens_n))*query_tokens_dict[token]
                
            scores.append([log(score_doc_len+1),
                           score_doc_token_max_tf,      
                           score_doc_token_min_tf,     
                           score_query_token_max_tf,    
                           score_query_token_min_tf,    
                           score_query_token_max_idf,   
                           score_query_token_min_idf,   
                           score_query_token_max_tfidf, 
                           score_query_token_min_tfidf, 
                           log(score_query_tf+1),
                           log(score_query_idf+1),      
                           score_query_tfidf,           
#                           score_query_bm25,            
                           log(score_query_lmir_jm+1),  
                           log(score_query_lmir_dir+1),
                           log(score_query_lmir_abs+1)])
        return scores


def obtain_features(docs,query):
    '''
    docs 是[doc1,doc2,doc3,...]
    '''
#    click_graph=pickle.load(open('click_graph','rb'))
    stop_char=['，','。','：','；','？','【','】','、','！','-',' ','（','）','[',']','(',')',',','.',';','?','!',':']
    query_tokens=[x for x in jieba.lcut(query) if x not in stop_char]
    feature_2=[]
    ltrModel_input=[]
    docs_id=[]
    for doc in docs:
        temp_features=[]
        summary_words = dict(Counter(doc['_source'].get('cut_words','')))
        ltrModel_input.append(summary_words)
        defectno = doc['_source'].get('defectNo',201501010000)
        defectid = doc['_source']['defectID']
        docs_id.append(defectid)
        create_date = doc['_source'].get('creationdate',0)
        current_code=doc['_source'].get('currentNode','0')

        
        attach_file_num_1 = log10(doc['_source'].get('att_file_num_1',0)+1)
        attach_file_num_3 = log10(doc['_source'].get('att_file_num_3',0)+1)
        attach_img_num_1 = log10(doc['_source'].get('att_img_num_1',0)+1)
        attach_img_num_3 = log10(doc['_source'].get('att_img_num_3',0)+1)
        
        es_score = log10(doc.get('_score',0)+1)
        
        temp_features.append(log10(log(click_graph.get(str(defectno),0)+1)+1))
        temp_features.append(log10(np.mean([click_graph.get((i,str(defectno)),0) for i in list(set(query_tokens))])+1))
        
        time_diff=(time.time()-create_date/1000.0)/(24*3600)      
        if time_diff<=30:
            temp_features += [1,0,0,0]
        elif time_diff<=90:
            temp_features += [0,1,0,0]
        elif time_diff<=180:
            temp_features += [0,0,1,0]
        else:
            temp_features += [0,0,0,1]
        
        if current_code >= '3':                
            temp_features.append(1)
        else:
            temp_features.append(0)
        temp_features.append(attach_file_num_1)       
        temp_features.append(attach_img_num_1)
        temp_features.append(attach_file_num_3)
        temp_features.append(attach_img_num_3)
        temp_features.append(es_score)
        feature_2.append(temp_features)
        
    m=FEATURES(ltrModel_input)
    features_1=m.feature_engine(query_tokens)
    
    return np.array([features_1[i]+feature_2[i] for i in range(len(docs))]),docs_id
    
    
    
def pred(es_file,query_file):
    x,docs_id=obtain_features(es_file,query_file)
#    model=pickle.load(open('model','rb'))   
    scores=model.predict(x)
    return [x[0] for x in sorted(zip(docs_id,scores),key=lambda x: x[1],reverse=True)]
    
    
def init_model():
    global click_graph,model
    click_graph=pickle.load(open('./SortAlgorithm/click_graph','rb'))
    model=pickle.load(open('./SortAlgorithm/model_new','rb'))
    

    
