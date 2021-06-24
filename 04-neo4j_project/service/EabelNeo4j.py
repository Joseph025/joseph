# -*- coding: utf-8 -*-

import configparser
import logging
import vertica_python
import py2neo
from neo4j import GraphDatabase
from py2neo import Graph,Node,Relationship,NodeMatcher,RelationshipMatch


graph = Graph(host="10.90.15.10",auth=('neo4j','admin'))
tx=graph.begin()

# 创建节点并建立关系
# node_a=Node('Persion', name='张三丰')
# node_b=Node('Persion', name='张无忌')
# ab=Relationship(node_a,'爷孙俩',node_b)
# graph.create(ab)


# 创建节点
# node = Node('Persion',name='张三')  # Persion为节点，name为节点属性，需要注意不要用label='label'否则label会成为节点的的属性
# node['hight'] = '175'    # 向node添加hight属性
# node.setdefault('age',18)    # 通过setdefault()方法赋值默认属性
# graph.create(node)    # 将节点加入图数据库与create不同之处在于若节点存在则不创建
# tx.commit()    # 提交图数据库的变更


# 对已存在的关系添加属性
# match (A)-[r:next]->(B)
#   ON CREATE SET r.duration = newDuration
#   ON MATCH  SET r.duration = CASE
#                                  WHEN r.duration > newDuration
#                                  THEN newDuration
#                                  ELSE r.duration
#                              END
# RETURN r.duration


# 查询节点的所有数据
# datas = graph.nodes.match("员工")

print(graph.nodes.get(1))
# first是找到最终客户的第一个节点
print(graph.nodes.match('最终客户').first())


# 可以根据自己写的cypher查询
# datas = graph.run('match(c:最终客户) return c limit 10').data()


# node节点查询，但是可按条件查询
# matcher=NodeMatcher(graph)
# datas = matcher.match('员工', emp_code='12468')


# 查询员工隶属部们关系,只取10条
# datas = graph.match(nodes=None, r_type='员工隶属部们', limit=10)
# 将所有关系全部取出来，limit=None标示不限制
# datas = graph.match(nodes=None, r_type=None, limit=None)


# for data in datas:
#     print(data)
