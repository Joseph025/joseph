#!/bin/bash
#pwd!@#

cd spark-job
spark-submit --files template_clip.txt --py-files data_clean.py --num-executors 8  --executor-cores 4 --executor-memory 16g  --driver-memory 16g --driver-cores 4  classifier_base_rule_spark.py
spark-submit  --files template_clip.txt --py-files data_clean.py --num-executors 8  --executor-cores 4 --executor-memory 16g  --driver-memory 16g --driver-cores 4 test_tools_predict.py 
sparrow --master yarn --num-executors 4  --executor-cores 4 --executor-memory 16g  --driver-memory 16g --driver-cores 4 -e "

create table if not exists quality_carbon_new.carbon_outbound_defect_category(
defectID string,
categories string,
loaded_timestamp string)
stored as parquet;
truncate table quality_carbon_new.carbon_outbound_defect_category;
insert into table quality_carbon_new.carbon_outbound_defect_category select * from quality_outbound_hive.outbound_defect_category;

create table if not exists quality_carbon_new.carbon_outbound_defect_test_tools(
defectID string,
test_tools string,
loaded_timestamp string)
stored as parquet;
truncate table quality_carbon_new.carbon_outbound_defect_test_tools;
insert into table quality_carbon_new.carbon_outbound_defect_test_tools select * from quality_outbound_hive.outbound_defect_test_tools;
"


