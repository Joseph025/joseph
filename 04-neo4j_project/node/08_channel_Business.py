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
    cur.execute("""SELECT m.code,
                      m.name,
                      m.generalLevel ,
                      m.finallevel,
                      m.crQuarterCode 
                    FROM bigdata.cdip_cragentinfo_channel m 
                    RIGHT JOIN 
                    (SELECT code, MAX(crQuarterCode) AS crQuarterCode  FROM cdip_cragentinfo_channel 
                      WHERE Newflag='1' AND dw_status='A' AND version='Y01' GROUP BY code) f 
                    ON m.code=f.code AND m.crQuarterCode =f.crQuarterCode
                    WHERE m.dw_status ='A' 
                    AND m.Newflag ='1' 
                    AND version='Y01'""")
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
                'label': '渠道商',
                'code': row[0],
                'name': row[1],
                'generalLevel': row[2],
                'finallevel': row[3],
                'crQuarterCode': row[4]
            })
            tx.run('create (channel_business:' + obj['label'] + '{code:$code,name:$name,'
                                                           'generalLevel:$generalLevel,finallevel:$finallevel,'
                                                           'crQuarterCode:$crQuarterCode})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node channel_business seccess')