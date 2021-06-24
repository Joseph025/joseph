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
    cur.execute("""SELECT  NVL(p.emp_code, cwf.emp_code) AS emp_code,
                      c.company_id 
                    FROM cms.cis_m_company c 
                    LEFT JOIN  cms.cis_d_person p 
                      ON c.principal_id=p.subject_id 
                      AND p.dw_status='A' 
                      AND LENGTH(p.emp_code)=5 
                      AND LEFT(p.emp_code, 1) BETWEEN '0' AND '9'
                    LEFT JOIN hrs.emp_m_cwf cwf 
                      ON TRIM(LOWER(c.principal_account))=TRIM(LOWER(cwf.domain_account)) 
                      AND cwf.dw_status='A'
                    WHERE  c.dw_status='A' 
                    AND NVL(p.emp_code, cwf.emp_code)  IS NOT NULL""")
    logging.info('get db view objects')
    # uri = "bolt://10.90.15.10:7687"
    # driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        while True:
            rows = cur.fetchmany(1000)
            if not rows: break
            for row in rows:
                relation = {
                    'emp_key': row[0],
                    'customer_key': row[1]
                }
                tx.run('MATCH (c1:最终客户{id:$customer_key}),(emp:员工{emp_code:$emp_key}) '
                    'merge (emp)-[r:员工负责客户{type:"员工负责客户"}]->(c1)', **relation)
                count = count + 1000
                if count >= 10000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()