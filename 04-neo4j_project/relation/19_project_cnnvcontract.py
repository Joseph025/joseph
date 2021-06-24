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
    cur.execute("""SELECT DISTINCT project_id, 
                      cnvccontractid  
                    FROM e2e.h3c_oe_headers_all 
                    WHERE cnvccontractid IS NOT NULL 
                    AND project_id IS NOT NULL
                    AND dw_status='A'""")

    logging.info('get db view objects')
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        while True:
            rows = cur.fetchmany(10000)
            if not rows: break
            for row in rows:
                relation = {
                    'project_key': row[0],
                    'cnvccontract_key': row[1]
                }
                tx.run('MATCH (cnvc:执行单{cnvccontractid:$cnvccontract_key}),(p1:pms_项目{PROJECT_ID:$project_key}) '
                       'create (p1)-[r:项目生成执行单{type:"项目生成执行单"}]->(cnvc)', **relation)
                count = count + 10000
                if count >= 100000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()