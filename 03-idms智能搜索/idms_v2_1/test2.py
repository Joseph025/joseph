# coding=utf-8
import requests
import json
import time
from datetime import datetime
import socket

"""
混合部署：service_tag: datasmart
独立部署：service_tag: hge
"""


class HgeClient:
    def __init__(self, cloudos_vip, username, password, keystone_port=5000, service_tag="datasmart"):
        self.__service_tag = service_tag
        self.__service_url_dict = {
            "datasmart": {
                "hge-graphdb": "os-dsmart-hge-graphdb-svc.cloudos-datasmart:8980",
                "hge-scheduler": "os-dsmart-hge-scheduler-svc.cloudos-datasmart:8983",
                "hge-graph-loader": "os-dsmart-hge-graph-loader-svc.cloudos-datasmart:8982",
                "hge-graph-query": "os-dsmart-hge-graph-query-svc.cloudos-datasmart:8984",
                "hge-graph-analysis": "os-dsmart-hge-graph-analysis-svc.cloudos-datasmart:8981"
            },
            "hge": {
                "hge-graphdb": "os-hge-graphdb-svc.cloudos-hge:8980",
                "hge-scheduler": "os-hge-scheduler-svc.cloudos-hge:8983",
                "hge-graph-loader": "os-hge-graph-loader-svc.cloudos-hge:8982",
                "hge-graph-query": "os-hge-graph-query-svc.cloudos-hge:8984",
                "hge-graph-analysis": "os-hge-graph-analysis-svc.cloudos-hge:8981"
            }
        }
        self.__host = cloudos_vip
        self.__port = keystone_port
        self.__username = username
        self.__password = password
        self.__local_cluster_mode = self.__get_client_mode()  # 是否当前集群中部署
        self.__headers = self.__get_headers()

    def __get_client_mode(self):
        """
        获取client模式
            1. 当调用API的程序运行在和hge相同集群时，直接通过服务名连接
            2. 当调用API的程序不运行在相同集群时，通过kong连接
        """
        graphdb_pod_name = self.__service_url_dict.get(self.__service_tag).get("hge-graphdb").split(":")[0]
        try:
            socket.getaddrinfo(graphdb_pod_name, "http")
            print("========= use local cluster mode =========")
            return True
        except:
            print("========= use external mode =========")
            return False

    def __get_headers(self):
        """
        获取headers
            1. 通过服务名进行连接时，需要设置userinfo
            2. 通过kong连接时，不用设置userinfo
        """
        headers = {
            "content-type": "application/json"
        }

        body = {
            "auth": {
                "identity": {
                    "methods": [
                        "password"
                    ],
                    "password": {
                        "user": {
                            "name": self.__username,
                            "domain": {
                                "name": "Default"
                            },
                            "password": self.__password
                        }
                    }
                }
            }
        }

        url = "http://{host}:{port}/v3/auth/tokens".format(
            host=self.__host, port=self.__port)
        token_select = requests.post(url, json=body, headers=headers)
        token_select_dict = token_select.json()

        token = token_select.headers.get("X-Subject-Token", "")
        hge_headers = {"X-Auth-Token": token}
        if self.__local_cluster_mode:
            user_id = token_select_dict.get(
                "token", {}).get("user", {}).get("id", "")
            user_name = token_select_dict.get(
                "token", {}).get("user", {}).get("name", "")
            role_list = token_select_dict.get("token", {}).get("roles", [])
            role_name = role_list[0].get("name", "") if len(role_list) > 0 else ""
            project_id = token_select_dict.get(
                "token", {}).get("project", {}).get("id", "")
            project_name = token_select_dict.get(
                "token", {}).get("project", {}).get("name", "")
            userinfo = [{
                "userId": user_id,
                "projectId": project_id,
                "roleName": role_name,
                "userName": user_name,
                "roleId": user_id,
                "projectName": project_name}]

            hge_headers["userinfo"] = json.dumps(userinfo)
        print(hge_headers)
        return hge_headers

    def __get(self, url, params={}):
        return requests.get(url, params=params, headers=self.__headers)

    def __post(self, url, body={}, params={}):
        return requests.post(url, json=body, params=params, headers=self.__headers)

    def __get_url(self, service_name, uri):
        if self.__local_cluster_mode:
            return "http://{host}/{uri}".format(
                host=self.__service_url_dict.get(self.__service_tag, {}).get(service_name), uri=uri.lstrip("/"))
        else:
            return "http://{vip}:11000/{tag}/v1.0/{service}/{uri}".format(vip=self.__host, tag=self.__service_tag,
                                                                          service=service_name, uri=uri.lstrip("/"))

    def __authorized(self, response):
        return int(response.status_code) != 401

    def get(self, service_name, uri, params={}):
        url = self.__get_url(service_name, uri)
        response = self.__get(url, params)
        if self.__authorized(response):
            return response
        else:
            self.__headers = self.__get_headers()
            return self.__get(url, params)

    def post(self, service_name, uri, body={}, params={}):
        url = self.__get_url(service_name, uri)
        response = self.__post(url, body, params)
        if self.__authorized(response):
            return response
        else:
            self.__headers = self.__get_headers()
            return self.__post(url, body, params)


if __name__ == '__main__':
    cmd = "g.V().has('name','{0}').label()".format("iFeatureV7B70")
    dict={"gremlin":cmd}
    rest_client = HgeClient("10.165.8.92", "dc_dev", "1qazxsw@")
    r=rest_client.get("hge-graphdb", "/api/graphs/dc_idms/gremlin?gremlin="+cmd+"")
    print(r.text)

    # print(rest_client.get("hge-graphdb", "/api/graphs/info"))
#
#     print(rest_client.get("hge-graph-query", "/api/graphs/t0325001/ui/gremlin?gremlin=g.V().count()"))

