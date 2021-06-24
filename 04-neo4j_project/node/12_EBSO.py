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
    cur.execute("""SELECT h.header_id,
                  h.order_type_id,
                  h.order_number,
                  h.ordered_date,
                  h.flow_status_code,
                  h.org_id,
                  h.type_flag,
                  h.deal_type 
                FROM e2e.h3c_oe_headers_all h 
                WHERE h.dw_status='A' 
                AND h.order_number IS NOT NULL""")
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
                'label': 'ebso',
                'header_id': row[0],
                'order_type_id': row[1],
                'order_number': row[2],
                'ordered_date': row[3],
                'flow_status_code': row[4],
                'org_id': row[5],
                'type_flag': row[6],
                'deal_type': row[7]
            })
            tx.run('create (ebso:' + obj['label'] + '{header_id:$header_id,order_type_id:$order_type_id,'
                                                           'order_number:$order_number,ordered_date:$ordered_date,'
                                                           'flow_status_code:$flow_status_code,org_id:$org_id,'
                                                       'type_flag:$type_flag})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node ebso seccess')