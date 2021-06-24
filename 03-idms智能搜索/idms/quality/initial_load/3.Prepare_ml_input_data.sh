#!/bin/bash
#pwd!@#
beeline -u "jdbc:hive2://10.165.8.52:10000/" -n testadmin -p "test1qaz@WSX" -e "
--内部表
drop table if exists quality_outbound_hive.hive_ml_model_data_prepare;
create table if not exists quality_outbound_hive.hive_ml_model_data_prepare(
defectID string, 
summery string, 
Content string, 
NodeCode string, 
defecttype string,
FeatureName string,
productlinename string)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_outbound_hive.outbound_defect_category;
create table if not exists quality_outbound_hive.outbound_defect_category(
defectID string,
categories string,
loaded_timestamp string)
row format DELIMITED
FIELDS TERMINATED BY '\b'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

drop table if exists quality_outbound_hive.outbound_defect_test_tools;
create table if not exists quality_outbound_hive.outbound_defect_test_tools(
defectID string,
test_tools string,
loaded_timestamp string)
row format DELIMITED
FIELDS TERMINATED BY '\b'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
"


