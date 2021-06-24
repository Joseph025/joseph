import sys
sys.path.append(r"/home/dc_dev/idms_v2/src")
from utils import common


def data_clean(s):
    s = str(s)
    if common.data_is_NULL(s):
        return ""
    s = s.replace('"',"'")
    s = s.replace(',',"。")
    s = s.replace('\n',"")
    s = s.replace('\r',"")
    return s


def put(data):
    # rddrow=data.rdd.map(lambda row: [[data_clean(row[0]), data_clean(row[1]), data_clean(row[2])]])
    # print(rddrow)

    # data是dataframe
    single=[]
    for row in data.toLocalIterator():
        dicts = {
            "name": data_clean(row[0]),
            "describe": data_clean(row[1]),
            "detail":data_clean(row[2])
        }
        single.append(dicts)
    return single

