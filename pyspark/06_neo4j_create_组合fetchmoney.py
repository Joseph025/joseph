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
uri = "bolt://10.90.15.10:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
with vertica_python.connect(**vertica_config) as connection:
    logging.info('get connection with vertica')
    # 创建连接数据库实例
    cur = connection.cursor()
    cur.execute("""SELECT  DISTINCT b.inventory_item_id,
                  a.ITEM_ID,
                  a.PRODUCT_CODE,
                  b.enabled_flag,
                  b.DESCRIPTION,
                  a.PRODUCT_TYPE,  
                  a.PRODUCTLINE 
                FROM bigdata.ebs_it_product_line a 
                LEFT JOIN  E2E.ebs_system_items_b B ON b.dw_status='A' AND b.ORGANIZATION_ID=473 AND b.segment1=a.item_id
                WHERE a.dw_status = 'A'
                AND a.PRODUCT_CODE NOT IN 
                  (SELECT segment1 FROM E2E.ebs_system_items_b WHERE dw_status='A' AND ORGANIZATION_ID=83) 
                UNION 
                SELECT b.inventory_item_id, 
                   a.ITEM_ID, 
                   a.PRODUCT_CODE, 
                   b.enabled_flag, 
                   b.DESCRIPTION, 
                   a.level3,  
                   a.level4 
                  FROM E2E.ebs_system_items_b B 
                  LEFT JOIN  BIGDATA.ebs_ct_bom_product_line a ON a.dw_status='A' AND b.segment1=a.item_id 
                  WHERE b.dw_status='A' AND b.ORGANIZATION_ID=83""")
    # 得到多行的元组((id,gts_code,cis_code,company_name,company_type),(id,gts_code,cis_code,company_name,company_type))
    logging.info('get db objects')
    # create neo4j connetion
    uri = 'neo4j://10.90.15.10:7687'
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"), encrypted=False)
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        while True:
            rows = cur.fetchmany(1000)
            if not rows: break
            for row in rows:
                obj = ({
                    'label': 'product',
                    'inventory_item_id': str(row[0]),
                    'ITEM_ID': row[1],
                    'PRODUCT_CODE': row[2],
                    'enabled_flag': row[3],
                    'DESCRIPTION': row[4],
                    'PRODUCT_TYPE': row[5],
                    'PRODUCTLINE': row[6]
                })
                tx.run('create (product:' + obj['label'] + '{inventory_item_id:$inventory_item_id,ITEM_ID:$ITEM_ID,'
                                                               'PRODUCT_CODE:$PRODUCT_CODE,enabled_flag:$enabled_flag,'
                                                               'DESCRIPTION:$DESCRIPTION,PRODUCT_TYPE:$PRODUCT_TYPE,'
                                                           'PRODUCTLINE:$PRODUCTLINE})',**obj)
                count = count + 1000
                if count >= 10000:
                    logging.info('submited.')
                    tx.commit()
                    count = 0
                    tx = session.begin_transaction()
        if count > 0:
            tx.commit()
    driver.close()
logging.info('create node product seccess')