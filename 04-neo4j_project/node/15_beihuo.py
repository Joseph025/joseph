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
    cur.execute("""SELECT DISTINCT orderno,
                   outdate,
                   receivedate,
                   province, city,
                   customeraddr,
                   CASE WHEN customeraddr LIKE '%自提%' THEN 1 ELSE 0 END AS mode 
                FROM E2E.dms_serialtrans 
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
                'label': '备货单',
                'orderno': row[0],
                'outdate': row[1],
                'receivedate': row[2],
                'province': row[3],
                'city': row[4],
                'customeraddr': row[5],
                'mode': row[6]
            })
            tx.run('create (choice:' + obj['label'] + '{orderno:$orderno,outdate:$outdate,'
                                                           'receivedate:$receivedate,province:$province,'
                                                           'city:$city,customeraddr:$customeraddr,'
                                                       'mode:$mode})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node 备货单 seccess')