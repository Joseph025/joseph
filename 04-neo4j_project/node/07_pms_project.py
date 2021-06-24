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
# 应该是数据平衡（先搁置）
# vertica_config['connection_load_balance'] = bool(vertica_config['connection_load_balance'])

objs = []


# get vertica objects
with vertica_python.connect(**vertica_config) as connection:
    logging.info('get connection with vertica')
    # 创建连接数据库实例
    cur = connection.cursor()
    cur.execute("""SELECT DISTINCT m.PROJECT_ID,
                      m.PROJECT_NAME,
                      m.REF_PROJECT_ID,
                      m.PROJECT_TYPE,
                      m.PROJECT_STAGE,
                      m.CREATED_DT,
                      m.ORDER_STATUS,
                      p.RESELLER_METHOD,
                      p.order_body 
                    FROM bigdata.pms_project_MASTER m 
                    LEFT JOIN bigdata.pms_project p ON p.PROJECT_ID=m.PROJECT_ID AND p.dw_status='A'
                    RIGHT JOIN (
                      SELECT PROJECT_ID, MAX(submit_time) AS submit_time 
                      FROM bigdata.pms_project 
                      WHERE dw_status='A' GROUP BY PROJECT_ID ) p1 ON p1.project_id = p.project_id AND p1.submit_time=p.submit_time
                    WHERE m.dw_status='A'""")
    # 得到多行的元组((id,gts_code,cis_code,company_name,company_type),(id,gts_code,cis_code,company_name,company_type))
    logging.info('get db objects')
    # create neo4j connetion
    uri = 'neo4j://10.90.15.10:7687'
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"), encrypted=False)
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        for row in cur.iterate():
            obj = ({
                'label': 'pms_项目',
                'PROJECT_ID': row[0],
                'PROJECT_NAME': row[1],
                'REF_PROJECT_ID': row[2],
                'PROJECT_TYPE': row[3],
                'PROJECT_STAGE': row[4],
                'CREATED_DT': row[5],
                'ORDER_STATUS': row[6],
                'RESELLER_METHOD': row[7],
                'order_body': row[8]
            })
            tx.run('create (pms_project:' + obj['label'] + '{PROJECT_ID:$PROJECT_ID,PROJECT_NAME:$PROJECT_NAME,'
                                                           'REF_PROJECT_ID:$REF_PROJECT_ID,PROJECT_TYPE:$PROJECT_TYPE,'
                                                           'PROJECT_STAGE:$PROJECT_STAGE,CREATED_DT:$CREATED_DT,'
                                                           'ORDER_STATUS:$ORDER_STATUS,'
                                                           'RESELLER_METHOD:$RESELLER_METHOD,order_body:$order_body})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node pms_project seccess')