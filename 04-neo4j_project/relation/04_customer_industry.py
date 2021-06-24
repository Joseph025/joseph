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
    cur.execute("""SELECT id,
                      NVL(NVL(industry3_id, industry2_id),industry1_id) as industry_id
                    FROM cms.CRM_T_COMPANY
                    WHERE dw_status='A' 
                    AND NVL(NVL(industry3_id, industry2_id),industry1_id) IS NOT NULL""")
    logging.info('get db view objects')
    uri = "bolt://10.90.15.10:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        while True:
            rows = cur.fetchmany(1000)
            if not rows: break
            for row in rows:

                relation = {
                    'customer_id_key': row[0],
                    'industry_id_key': row[1]
                }
                tx.run('match (c1:最终客户{id:$customer_id_key}),(ind:行业{industry_id:$industry_id_key}) '
                       'merge (c1)-[r:客户归属行业{type:"客户归属行业"}]->(ind)', **relation)
                count = count + 1000
                if count >= 2000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()