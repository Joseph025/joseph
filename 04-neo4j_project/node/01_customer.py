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

objs = []

# get vertica objects
with vertica_python.connect(**vertica_config) as connection:
    logging.info('get connection with vertica')
    # 创建连接数据库实例
    cur = connection.cursor()
    cur.execute("""SELECT  id,
                  gts_code,
                  cis_code,
                  company_name,
                  company_type 
                FROM cms.CRM_T_COMPANY 
                WHERE dw_status='A'
                UNION 
                SELECT NVL(c.company_id, d.merge_gts_code) AS id, 
                  d.merge_gts_code, 
                  d.merge_cis_code, 
                  d.merge_company_name, 
                  '-1' AS company_type 
                FROM cms.CRM_T_DUPLICATE d
                LEFT JOIN cms.cis_m_company c ON d.merge_cis_code IS NOT NULL 
                    AND d.merge_cis_code=c.company_code 
                    AND c.dw_status='A'""")
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
                'label': '最终客户',
                'id': row[0],
                'gts_code': row[1],
                'cis_code': row[2],
                'company_name': row[3],
                'company_type': row[4]
            })
            tx.run('create (customer:' + obj['label'] + '{id:$id, gts_code:$gts_code, '
                                                'cis_code:$cis_code, company_name:$company_name, '
                                                'company_type:$company_type})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node customer seccess')