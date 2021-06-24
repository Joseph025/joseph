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
    cur.execute("""select distinct * from (SELECT emp_code,
                  dept_code 
                FROM hrs.emp_f_base 
                WHERE dw_status='A'
                UNION ALL
                SELECT emp_code, 
                  dept_code 
                FROM hrs.emp_m_cwf 
                WHERE dw_status='A') a""")

    logging.info('get db view objects')
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        while True:
            rows = cur.fetchmany(1000)
            if not rows: break
            for row in rows:
                relation = {
                    'emp_key': row[0],
                    'dept_key': row[1]
                }
                tx.run('MATCH (emp:员工{emp_code:$emp_key}),(dept:有效部们{dept_code:$dept_key}) '
                       'merge (emp)-[r:员工隶属部们{type:"员工隶属部们"}]->(dept)',**relation)

                count = count + 1000
                if count >= 10000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()


