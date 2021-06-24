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
    cur.execute("""SELECT 'SFDC' AS source_system,
                      case_number,
                      subject,
                      case_status,
                      priority,
                      case_Type,
                      resolution_type,
                      Created_date,
                      Closed_date 
                    FROM sfdc.ts_sfdc_case_all_f""")
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
                'label': 'sfdc_400问题单',
                'source_system': row[0],
                'case_number': row[1],
                'subject': row[2],
                'case_status': row[3],
                'priority': row[4],
                'case_Type': row[5],
                'resolution_type': row[6],
                'Created_date': row[7],
                'Closed_date': row[8]
            })
            tx.run('create (sfdc_400:' + obj['label'] + '{source_system:$source_system,case_number:$case_number,'
                                                           'subject:$subject,case_status:$case_status,'
                                                           'priority:$priority,case_Type:$case_Type,'
                                                       'resolution_type:$resolution_type,Created_date:$Created_date,Closed_date:$Closed_date})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node sfdc_400 seccess')