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
    cur.execute("""SELECT  DISTINCT  c.PROJECT_ID,
                      i.industry_id 
                    FROM bigdata.pms_project_MASTER  c 
                    LEFT JOIN cms.cis_d_industry i ON NVL(NVL(c.INDUSTRY3,c.INDUSTRY2),c.INDUSTRY1) = i.pms_industry_id
                      AND i.dw_status='A'
                    WHERE  c.dw_status='A'""")

    logging.info('get db view objects')
    uri = 'neo4j://10.90.15.10:7687'
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"), encrypted=False)
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        for row in cur.iterate():
            relation = {
                'project_key': row[0],
                'industry_key': row[1]
            }
            tx.run('MATCH (ind:行业{industry_id:$industry_key}),(project:pms_项目{PROJECT_ID:$project_key}) '
                   'create (project)-[r:项目归属行业{type:"项目归属行业"}]->(ind)',**relation)

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



