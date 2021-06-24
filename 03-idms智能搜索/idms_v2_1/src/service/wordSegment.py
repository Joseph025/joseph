import jieba
from src.utils import common
import os


g_word_segment_local_file  = os.path.join(os.path.dirname(os.getcwd()), \
                                          'idms_v2', \
                                          'cfg', \
                                          'wegDict.txt')


def jieba_load_userdict():
    jieba.load_userdict(g_word_segment_local_file)


g_drpoList = ['[',']',':',' ','+','-','(',')','.','!',',','{','}']


def run(srcString, bUseCustomDict=False):
    # param是字典型
    global g_word_segment_local_file
    global g_drpoList
    if common.data_is_NULL(srcString):
        return []
    # load user custom word dict
    if bUseCustomDict:
        # 对简述分词
        jieba.load_userdict(g_word_segment_local_file)
    # Data preprocessing数据预处理
    user_string = common.C_trans2_E(srcString)
    # segment将数据生成一个list
    ret_list = jieba.lcut(user_string.lower())
    wordList = []
    for i in ret_list:
        if i in wordList:
            continue
        if i in g_drpoList:
            continue
        if len(i) < 2:
            continue
        wordList.append(i)
    return wordList