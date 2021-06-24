#!/bin/bash

cur_dir=/opt/apps/idms/richtext_attached_info

cd ${cur_dir}

hive2 -e "drop table if exists quality_outbound_hive.tmp_richtext_attached_info;"

hive2 -e "create table quality_outbound_hive.tmp_richtext_attached_info as \
select a.* from quality_outbound_hive.richtext_attached_info a left join quality_landing_hive.richtext_incr b on a.richtextid = b.richtextid where b.richtextid is null"

spark-submit --master yarn --conf spark.executorEnv.PYTHONHASHSEED=321 --driver-memory 8g --executor-memory 4g --num-executors 15 --executor-cores 3 richtext_attached_info.py

