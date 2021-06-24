#!/bin/bash

export ML_PATH=/usr/local/quality/ml/spark-job/
cd $ML_PATH
spark-submit --py-files "$ML_PATH"data_clean.py --num-executors 4 --driver-memory 16g --executor-memory 8g "$ML_PATH"classifier_base_rule_spark.py
spark-submit --py-files "$ML_PATH"data_clean.py --num-executors 4 --driver-memory 16g --executor-memory 8g "$ML_PATH"test_tools_predict.py

sparrow --master yarn --num-executors 4  --executor-cores 4 --executor-memory 4g  --driver-memory 8g --driver-cores 4 -e "
truncate table quality_carbon_new.carbon_outbound_defect_category;
insert into table quality_carbon_new.carbon_outbound_defect_category select * from quality_outbound_hive.outbound_defect_category;

truncate table quality_carbon_new.carbon_outbound_defect_test_tools;
insert into table quality_carbon_new.carbon_outbound_defect_test_tools select * from quality_outbound_hive.outbound_defect_test_tools;

"

