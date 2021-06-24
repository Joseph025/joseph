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
    cur.execute("""SELECT cnvccontractid ,
                   cnvcnoversionid,
                   cndtprocessdate,
                   cndtprojectwantedreceivedate,      
                   cndtupdatedate,
                   cndinputprice,
                   cntisumflag 
            FROM e2e.cdip_tbcontract2       
            WHERE dw_status = 'A'
            AND cnvcprojectid IS NOT NULL""")
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
                'label': '执行单',
                'cnvccontractid': row[0],
                'cnvcnoversionid': row[1],
                'cndtprocessdate': row[2],
                'cndtprojectwantedreceivedate': row[3],
                'cndtupdatedate': row[4],
                'cndinputprice': str(row[5]),
                'cntisumflag': row[6]
            })
            tx.run('create (cnvccontract:' + obj['label'] + '{cnvccontractid:$cnvccontractid,cnvcnoversionid:$cnvcnoversionid,'
                                                           'cndtprocessdate:$cndtprocessdate,cndtprojectwantedreceivedate:$cndtprojectwantedreceivedate,'
                                                           'cndtupdatedate:$cndtupdatedate,cndinputprice:$cndinputprice,cntisumflag:$cntisumflag})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node cnvccontract seccess')