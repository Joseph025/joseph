#!/bin/bash
#pwd!@#
beeline -u "jdbc:hive2://10.64.8.42:10000/" -n dcprodworker -p "worker12@WSX" -e "
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
FIELDS TERMINATED BY ‘\b’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE;

drop table if exists quality_outbound_hive.outbound_defect_test_tools;
create table if not exists quality_outbound_hive.outbound_defect_test_tools(
defectID string,
test_tools string,
loaded_timestamp string)
row format DELIMITED
FIELDS TERMINATED BY ‘\b’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE;

"

sparrow --master yarn --num-executors 12  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 -e "
--设置并行度，输出为100个文件
set spark.sql.shuffle.partitions=100;

insert into table quality_outbound_hive.hive_ml_model_data_prepare
SELECT distinct m.defectID, m.summery, rt.Content, t.NodeCode, m.defecttype, pf.FeatureName, pl.productlinename FROM quality_carbon_new.carbon_landing_DefectMain m 
              LEFT JOIN quality_carbon_new.carbon_landing_DefectProcessFlow f ON m.DefectID = f.DefectID
              LEFT JOIN quality_carbon_new.carbon_landing_workflowTask t ON t.WorkflowInstanceID = f.WorkflowInstanceID
              LEFT JOIN quality_carbon_new.carbon_landing_WorkflowRecord r ON r.WorkflowTaskID = t.WorkflowTaskID
              LEFT JOIN quality_landing_hive.richtext_all_std rt ON rt.RichTextID = r.Comments
              LEFT JOIN quality_carbon_new.carbon_landing_T_ProductFeature pf ON m.SubFearureID = pf.SubFeatureID
              LEFT JOIN quality_carbon_new.carbon_landing_productinfovr vr on vr.productinfovrid = m.releaseid
              LEFT JOIN quality_carbon_new.carbon_landing_productline pl on pl.productlineid = vr.productlineid
where t.NodeCode=1 or t.NodeCode=3;
"

