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
    cur.execute("""SELECT DISTINCT cust_po_number,
                 order_number 
                FROM e2e.h3c_oe_headers_all
                WHERE dw_status='A' 
                AND order_number IS NOT NULL 
                AND cust_po_number IS NOT NULL """)

    logging.info('get db view objects')
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        while True:
            rows = cur.fetchmany(1000)
            if not rows: break
            for row in rows:
                relation = {
                    'cust_po_number_key': row[0],
                    'order_number_key': row[1]
                }
                tx.run('MATCH (contract:合同{cust_po_number:$cust_po_number_key}),(ebso:ebso{order_number:$order_number_key}) '
                       'create (contract)-[r:销售合同生成ebso{type:"销售合同生成ebso"}]->(ebso)', **relation)
                count = count + 1000
                if count >= 100000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()