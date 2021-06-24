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
    cur.execute("""SELECT oldC.cnvccontractid ,
                           newC.cnvccontractid    
                    FROM e2e.cdip_tbcontract2 oldC
                    LEFT JOIN 
                    (SELECT cnvccontractid, cnvcnoversionid FROM e2e.cdip_tbcontract2       
                    WHERE dw_status = 'A' AND cnvcprojectid IS NOT NULL AND cntisumflag = 1 ) newC 
                    ON oldC.cnvcnoversionid =newC.cnvcnoversionid 
                    WHERE oldC.dw_status = 'A'
                    AND oldC.cnvcprojectid IS NOT NULL
                    AND oldC.cntisumflag = 0
                    AND newC.cnvccontractid IS NOT NULL""")
    logging.info('get db view objects')
    uri = "bolt://10.90.15.10:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
    with driver.session() as session:
        count=0
        tx = session.begin_transaction()
        while True:
            rows=cur.fetchmany(5000)
            if not rows:break
            for row in rows:
                relation = {
                    'old_key': row[0],
                    'new_key': row[1]
                }
                tx.run('MATCH (old:执行单{cnvccontractid:$old_key}),(new:执行单{cnvccontractid:$new_key}) '
                       'create (old)-[r:执行单改成{type:"执行单改成"}]->(new)',**relation)

                count = count + 5000
                if count >= 20000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
session.close()