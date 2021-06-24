# -*- coding: utf-8 -*-

import configparser
import logging
from pyhive import hive
import vertica_python
from neo4j import GraphDatabase
# import SqlExtracter as sqlex
# import KettleClass as kcs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s')

# 获取vertica连接配置
logging.info('load db config')
config = configparser.ConfigParser()
config.read('/home/dc_dev/access.conf', encoding='utf-8')
hive_config = dict(config.items('hive'))


objs = []


# get vertica objects
with hive.Connection(**hive_config) as connection:
    logging.info('get connection with hive')
    # 创建连接数据库实例
    cur = connection.cursor()
    cur.execute("""SELECT 'CRM' AS  source_system,
    d.new_name,
    d.new_title,
    d.new_casestauts,
    d.new_caselevel,
    d.new_faultreasonName ,
    d.new_casetype,
    CONCAT(NVL(d.new_isonsite,FALSE), CONCAT('-',NVL(d.new_isreplace_part,FALSE))) as new_types, 
    d.createdon,
    d.new_closetime,
    d.new_customername
FROM (
	SELECT 
	new_name, 
	new_title, 
	new_casestauts,
	new_caselevel,
	new_faultreasonName ,
	new_casetype,
	new_isonsite,
	new_isreplace_part,
	createdon, 
	new_closetime,
	new_customername,
	ROW_NUMBER() OVER(partition BY new_name ORDER BY modifiedon DESC) AS rn
	FROM quality_landing_hive.v_new_case 
) d 
WHERE d.rn = 1""")

    logging.info('get db objects')
    # create neo4j connetion

    uri = 'neo4j://10.90.15.10:7687'
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"), encrypted=False)
    with driver.session() as session:
        count = 0
        tx = session.begin_transaction()
        for row in cur.iterate():
            obj = ({
                'label': 'crm_hive_400问题单',
                'source_system': row[0],
                'new_name': row[1],
                'new_title': row[2],
                'new_casestauts': row[3],
                'new_caselevel': row[4],
                'new_faultreasonName': row[5],
                'new_casetype': row[6],
                'new_types': row[7],
                'createdon': row[8],
                'new_closetime': row[9],
                'new_customername': row[10]
            })
            tx.run('create (hive_400:' + obj['label'] + '{source_system:{source_system},new_name:{new_name},new_title:{new_title}'
                                                        'new_casestauts:{new_casestauts},new_caselevel:{new_caselevel},'
                                                        'new_faultreasonName:{new_faultreasonName},new_casetype:{new_casetype},'
                                                        'new_types:{new_types},createdon:{createdon},new_closetime:{new_closetime},'
                                                        'new_customername:{new_customername}})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count > 0:
            tx.commit()
    driver.close()
logging.info('create node hive_400 seccess')