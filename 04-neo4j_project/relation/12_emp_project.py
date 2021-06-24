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


with vertica_python.connect(**vertica_config) as connection:
    logging.info('get connection with vertica')
    cur = connection.cursor()
    cur.execute("""SELECT  DISTINCT NVL(cwf.emp_code, EMPLOYE_ID) AS emp_code,
              c.PROJECT_ID 
            FROM bigdata.pms_project_MASTER  c 
            LEFT JOIN hrs.emp_m_cwf cwf 
              ON TRIM(LOWER(c.EMPLOYE_ID))=TRIM(LOWER(cwf.notes_id)) 
              AND cwf.dw_status='A'
              AND NOT (LENGTH(EMPLOYE_ID)=5 AND LEFT(EMPLOYE_ID, 1) BETWEEN '0' AND '9')
            WHERE  c.dw_status='A' 
            AND NVL(cwf.emp_code, EMPLOYE_ID)  IS NOT NULL""")

    logging.info('get db view objects')
    uri = 'neo4j://10.90.15.10:7687'
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"), encrypted=False)
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        for row in cur.iterate():
            relation = {
                'emp_key': row[0],
                'project_key': row[1]
            }
            tx.run('MATCH (emp:员工{emp_code:$emp_key}),(project:pms_项目{PROJECT_ID:$project_key}) '
                   'create (emp)-[r:员工报单项目{type:"员工报单项目"}]->(project)',**relation)

            count = count + 1
            if count >= 10000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count > 0:
            tx.commit()
    driver.close()
logging.info('get relatition success')



