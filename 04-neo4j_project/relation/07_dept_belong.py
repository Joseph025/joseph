# -*- coding: utf-8 -*-

import configparser
import logging
import vertica_python
from neo4j import GraphDatabase
# import SqlExtracter as sqlex
# import KettleClass as kcs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s')

# 获取vertica连接配置
logging.info('load db config')
config = configparser.ConfigParser()
config.read('E:\\MySelfcode\\h3c\\neo4j\\access.conf', encoding='utf-8')
vertica_config = dict(config.items('vertica'))
vertica_config['backup_server_node'] = vertica_config['backup_server_node'].split(',')


uri = "bolt://10.90.15.10:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
with vertica_python.connect(**vertica_config) as connection:
    logging.info('get connection with vertica')
    cur = connection.cursor()
    cur.execute("""SELECT dept_code,
          sup_dept_code 
        FROM hrs.emp_t_departments""")
    res = cur.fetchall()
    logging.info('get db view objects')
    with driver.session() as session:
        tx = session.begin_transaction()
        for row in res:
            relation = {
                'dept_key': row[0],
                'sup_dept_key': row[1]
            }
            tx.run('MATCH (d1:有效部们{dept_code:$dept_key}),(d2:有效部们{dept_code:$sup_dept_key}) '
                   'merge (d1)-[r:部们隶属{type:"部们隶属"}]->(d2)',**relation)

        logging.info('get relatition success')
        tx.commit()
session.close()


