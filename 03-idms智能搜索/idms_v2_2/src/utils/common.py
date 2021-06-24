import sys
sys.path.append("/home/dc_dev/idms_v2/")
import os
import logging


def data_is_NULL(data):
    return not data


def C_trans2_E(s):
    E_pun = u',.!?[]()<>"\''
    C_pun = u'，。！？【】（）《》“、‘'
    table= {ord(f):ord(t) for f,t in zip(C_pun,E_pun)}
    return s.translate(table)


def read_file_lines_to_list(F):
    if not os.path.exists(F):
        logging.error("input file not exists <%s>"%F)
    with open(F,"r",encoding = "utf-8") as f:
        retStr = f.read()
        retList = retStr.split('\n')
        f.close()
    return retList



def csv_list_str_2_list(s):
    if data_is_NULL(s):
        return []
    if '[' in s or ']' in s:
        s = s[1:-1]
        if data_is_NULL(s):
            return []
        return s.split(",")
    else:
        if s.strip():
            l = []
            l.append(s)
            return l
        else:
            return []


def list_str_2_list(s,splitChar):
    if data_is_NULL(s):
        return s
    retList = []
    strList = s.split(splitChar)
    for str in strList:
        retList.append(str)
    return retList
