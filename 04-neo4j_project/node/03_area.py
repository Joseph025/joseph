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
    cur.execute("""SELECT region_id,
                      pms_region_id,
                      sip_region_id,
                      region_lvl,
                      region_name,
                      region_code 
                    FROM cms.CIS_D_REGION
                    WHERE dw_status='A'""")
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
                'label': '地区',
                'region_id': row[0],
                'pms_region_id': row[1],
                'sip_region_id': row[2],
                'region_lvl': row[3],
                'region_name': row[4],
                'region_code': row[5]
            })
            tx.run('create (area:' + obj['label'] + '{region_id:$region_id, pms_region_id:$pms_region_id, '
                                                'sip_region_id:$sip_region_id, region_lvl:$region_lvl, '
                                                'region_name:$region_name,region_code:$region_code})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node area seccess')