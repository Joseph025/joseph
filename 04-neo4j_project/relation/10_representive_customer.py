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
    cur.execute("""select distinct * from (SELECT office.agentoffice,
                    a.id 
                    FROM cms.crm_t_company a
                    LEFT JOIN bigdata.agent_office office ON office.dw_status='A' AND a.office_name=office.agentOffice_1
                    WHERE a.office_name IS NOT NULL
                     UNION ALL
                    SELECT distinct b.agentOffice,
                    c.id 
                    FROM cms.crm_t_company c
                    LEFT JOIN bigdata.agent_office b ON 
                        CASE WHEN c.tsoffice_name LIKE '%行业%' THEN '总部' 
                        ELSE REPLACE(REPLACE(c.tsoffice_name,'代表处', ''),'服务部', '') END = b.representiveOffice 
                            AND b.dw_status='A'
                    WHERE c.dw_status='A'
                    AND b.agentOffice IS NOT NULL) a""")
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
                    'agent_key': row[0],
                    'customer_key': row[1]
                }
                tx.run('MATCH (c1:最终客户{id:$customer_key}),(a1:代表处{agentOffice:$agent_key}) '
                    'merge (a1)-[r:代表处负责客户{type:"代表处负责客户"}]->(c1)', **relation)
                count = count + 1000
                if count >= 10000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()