#!/bin/bash

cur_dir=/opt/apps/idms/idms_log_recall

cd ${cur_dir}

search_file=/apps/hdfs/quality/inbound/idms/idms-search/idms-search_$(date +%Y%m%d).csv
click_file=/apps/hdfs/quality/inbound/idms/idms-ticket/idms-ticket_$(date +%Y%m%d).csv

rm -f idms-search.csv
rm -f idms-click.csv

hadoop fs -get ${search_file} idms-search.csv
hadoop fs -get ${click_file} idms-click.csv

spark-submit --master yarn --conf spark.executorEnv.PYTHONHASHSEED=321 --driver-memory 8g --executor-memory 4g --num-executors 15 --executor-cores 3 log_recall.py

rm -f idms-search.csv
rm -f idms-click.csv
