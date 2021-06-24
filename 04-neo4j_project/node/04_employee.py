# -*- coding: utf-8 -*-

import configparser
import logging
import vertica_python
from neo4j import GraphDatabase
from datetime import datetime
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
    cur.execute("""SELECT 'REG' AS emp_type,
  emp_code,
  emp_name,
  sex,
  birthday_date,
  entry_date,
  NULL AS attrition_date,
  site,
  domain_account,
  display_name,
  mobile 
FROM hrs.emp_m_essential 
WHERE dw_status='A'
UNION
SELECT 'REG' AS emp_type,
  a.emp_code,
  a.emp_name, 
  a.sex, 
  a.birthday_date, 
  a.entry_date, 
  b.leave_date,
  a.workarea, 
  c.domain_account, 
  c.display_name,
  a.mobile
FROM hrs.emp_f_base a 
  RIGHT JOIN hrs.emp_t_dimission b ON a.emp_code=b.emp_code AND a.dw_status='A' 
  LEFT JOIN (
    SELECT emp_code, MAX(domain_account) AS domain_account, MAX(LOWER(CONCAT(last_name,first_name))) AS display_name 
    FROM hrs.emp_f_wx_account_info WHERE dw_status='A' GROUP BY emp_code) c 
  ON a.emp_code=c.emp_code AND a.dw_status='A' 
UNION
SELECT 'CWF' AS emp_type,
  emp_code,
  emp_name,
  sex,
  birthday_date,
  entry_date,
  attend_enddate,
  site,
  domain_account,
  display_name,
  mobile
FROM hrs.emp_m_cwf
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
                'label': '员工',
                'emp_type': row[0],
                'emp_code': row[1],
                'emp_name': row[2],
                'sex': row[3],
                'birthday_date': row[4],
                'entry_date': row[5],
                'attrition_date': row[6],
                'site': row[7],
                'domain_account': row[8],
                'display_name': row[9],
                'mobile': row[10]
            })
            tx.run('create (employee:' + obj['label'] + '{emp_type:$emp_type, emp_code:$emp_code,emp_name:$emp_name, '
                                                        'sex:$sex,birthday_date:$birthday_date,entry_date:$entry_date,'
                                                        'attrition_date:$attrition_date,site:$site,'
                                                        'domain_account:$domain_account,display_name:$display_name,'
                                                        'mobile:$mobile})',**obj)
            count = count + 1
            if count >= 2000:
                logging.info('submited.')
                tx.commit()
                count = 0
                tx = session.begin_transaction()
        if count> 0:
            tx.commit()
    driver.close()
logging.info('create node employee seccess')


