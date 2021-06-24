import urllib3
from urllib.parse import urlencode
import json
import requests
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO) 

value = {
			"from": 30,
			"size": 15,
			"sort":[{"_score": "desc"},

					{"defectID": "desc"}],

			"query":{"bool": {"must": [{
										"match": {
												"summary": "【SR88 V7R7】【SR88 B75分支】【NGMVPN 严重】入PE设备带4K隧道16K转发表项，BGP配置GR后主备倒换，倒换完成后删除选择性隧道指定acl中的rule，选择性隧道无法删除。"
												}
									}]
							}
					}
			}
url = "http://127.0.0.1:5000/testAPI"
req_headers = {"content-type":"application/json; charset=UTF-8"}

def post_req1():
	http = urllib3.PoolManager()
	encode_args = json.dumps(value).encode("utf-8")

	r = http.request(
					"POST",
					url,
					body = encode_args,
					headers = req_headers)
	print(res.json())

def post_req2():
	req_body = json.dumps(value)
	res = requests.post(url,headers=req_headers,data=req_body)
	print(res.json())

if __name__=="__main__":

	# post_req1()

	# for i in range(10):
	post_req2()