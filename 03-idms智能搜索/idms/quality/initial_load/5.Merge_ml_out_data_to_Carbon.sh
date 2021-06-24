#!/bin/bash
#pwd!@#

sparrow --master yarn --num-executors 8  --executor-cores 4 --executor-memory 8g  --driver-memory 8g --driver-cores 4 -e "
truncate table quality_carbon_new.carbon_outbound_defect_category;
create table if not exists quality_carbon_new.carbon_outbound_defect_category(
defectID string,
categories string,
loaded_timestamp timestamp)
stored as parquet;
insert into table quality_carbon_new.carbon_outbound_defect_category select * from quality_outbound_hive.outbound_defect_category;

truncate table quality_carbon_new.carbon_outbound_defect_test_tools;
create table if not exists quality_carbon_new.carbon_outbound_defect_test_tools(
defectID string,
test_tools string,
loaded_timestamp timestamp)
stored as parquet;
insert into table quality_carbon_new.carbon_outbound_defect_test_tools select * from quality_outbound_hive.outbound_defect_test_tools;
"

