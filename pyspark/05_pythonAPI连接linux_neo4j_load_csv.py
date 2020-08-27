# -*- coding: utf-8 -*-

import configparser
import logging
import vertica_python
from neo4j import GraphDatabase
import csv
import paramiko
# import SqlExtracter as sqlex
# import KettleClass as kcs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s')

# 获取vertica连接配置
logging.info('load db config')
config = configparser.ConfigParser()
config.read('E:\\MySelfcode\\h3c\\neo4j\\access.conf', encoding='utf-8')
vertica_config = dict(config.items('vertica'))
vertica_config['backup_server_node'] = vertica_config['backup_server_node'].split(',')


with vertica_python.connect(**vertica_config) as connection:
    logging.info('get connection with vertica')
    ip = "10.90.15.10"
    port = 22
    user = "root"
    password = "MFmg5pmQsSeK"
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, port, user, password)
    # file_name = ssh.exec_command("/opt/neo4j/import/relation.csv")
    file_name='E:\\MySelfcode\\h3c\\neo4j\\relation.csv'
    with open(file_name, 'w', encoding='utf-8', newline='\n') as f:
        cur = connection.cursor()
        head=[]
        write = csv.writer(f)

        count=cur.execute("""SELECT id,
                      parent_id 
                    FROM cms.CRM_T_COMPANY
                    WHERE dw_status='A'""")
        for index in count.description:
            head.append(index[0])
        head = tuple(head)
        write.writerow(head)
        while True:
            rows = cur.fetchmany(10000)
            if not rows: break
            for row in rows:
                write.writerow(row)
        logging.info("文件创建完成")
    uri = "bolt://10.90.15.10:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
    with driver.session() as session:
        tx = session.begin_transaction()
        sftp = ssh.open_sftp()
        sftp.put(r'E:\\MySelfcode\\h3c\\neo4j\\relation.csv','/opt/neo4j/import/relation.csv')
        tx.run('LOAD CSV WITH HEADERS FROM "file:///relation.csv" AS line'
               ' match(c1:customer{id:line.id}),(c2:customer{id:line.parent_id}) '
               'merge (c1)-[r:客户父子]->(c2)')

session.close()


