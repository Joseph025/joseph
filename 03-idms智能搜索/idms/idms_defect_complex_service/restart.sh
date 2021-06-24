#!/bin/bash
source /etc/profile

run_dir=/opt/apps/idms/idms_defect_service

cd ${run_dir}

for pid in $(ps aux | grep optimization-ws.py | grep -v grep | awk '{print $2}')
do
kill -9 $pid
done

spark-submit --master yarn --conf spark.executorEnv.PYTHONHASHSEED=321 --driver-memory 8g --executor-memory 4g --num-executors 15 --executor-cores 3 click_graph_cal.py

sh start.sh

blank=$(echo -e "\n")
echo $blank
