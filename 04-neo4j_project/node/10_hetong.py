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
    cur.execute("""SELECT  h.cust_po_number, 
                    MAX(h.ordered_date) AS ordered_date,
                    MAX(h.currency) AS currency,
                    SUM(l.unit_selling_price*qty) AS order_amount,
                    SUM(l.unit_selling_price*shipped_qty) AS shipped_amount,
                    SUM(ar_amount) AS ar_amount 
                    FROM e2e.h3c_oe_headers_all h 
                    LEFT JOIN e2e.h3c_oe_lines_all l ON h.header_id=l.header_id AND l.dw_status='A'
                    WHERE h.dw_status='A' AND h.cust_po_number IS NOT NULL
                    GROUP BY h.cust_po_number""")
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
                'label': '合同',
                'cust_po_number': row[0],
                'ordered_date': row[1],
                'currency': row[2],
                'order_amount': str(row[3]),
                'shipped_amount': str(row[4]),
                'ar_amount': str(row[5])
            })
            tx.run('create (contract:' + obj['label'] + '{cust_po_number:$cust_po_number,ordered_date:$ordered_date,'
                                                           'currency:$currency,order_amount:$order_amount,'
                                                           'shipped_amount:$shipped_amount,ar_amount:$ar_amount})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node contract seccess')