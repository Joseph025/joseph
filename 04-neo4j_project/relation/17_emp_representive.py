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
    cur.execute("""select distinct * from(SELECT DISTINCT salesman_id,
             o.agentOffice 
            FROM sales_dashboard_salesman s
            LEFT JOIN bigdata.agent_office o ON  s.representiveOffice=o.representiveOffice AND o.dw_status='A'
            WHERE s.dw_status='A'  
            AND s.salesman_id IN (SELECT  emp_code FROM hrs.emp_f_base WHERE  dw_status='A')
            UNION ALL
            SELECT DISTINCT cwf.emp_code, o.agentOffice 
            FROM sales_dashboard_salesman s
            LEFT JOIN bigdata.agent_office o ON  s.representiveOffice=o.representiveOffice AND o.dw_status='A'
            LEFT JOIN hrs.emp_m_cwf cwf ON cwf.dw_status='A' AND UPPER(s.salesman_id) = UPPER(cwf.notes_id)
            WHERE s.dw_status='A') a""")
    res = cur.fetchall()
    logging.info('get db view objects')
    with driver.session() as session:
        tx = session.begin_transaction()
        for row in res:
            relation = {
                'emp_key': row[0],
                'agent_key': row[1]
            }
            tx.run('MATCH (emp:员工{emp_code:$emp_key}),(agent:代表处{agentOffice:$agent_key}) '
                   'create (emp)-[r:员工归属代表处{type:"员工归属代表处"}]->(agent)',**relation)

        logging.info('get relatition success')
        tx.commit()
session.close()


