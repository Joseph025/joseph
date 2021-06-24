#!/bin/bash

sparrow --master yarn --num-executors 8  --executor-cores 4 --executor-memory 8g --driver-memory 8g --driver-cores 4 -e "

truncate table  quality_outbound_hive.hive_ml_model_data_prepare;

insert into table quality_outbound_hive.hive_ml_model_data_prepare
SELECT distinct m.defectID, m.summery, rt.Content, t.NodeCode, m.defecttype, pf.FeatureName, pl.productlinename FROM quality_carbon_new.carbon_landing_DefectMain m
              LEFT JOIN quality_carbon_new.carbon_landing_DefectProcessFlow f ON m.DefectID = f.DefectID
              LEFT JOIN quality_carbon_new.carbon_landing_workflowTask t ON t.WorkflowInstanceID = f.WorkflowInstanceID
              LEFT JOIN quality_carbon_new.carbon_landing_WorkflowRecord r ON r.WorkflowTaskID = t.WorkflowTaskID
              LEFT JOIN quality_landing_hive.richtext_stg rt ON rt.RichTextID = r.Comments
              LEFT JOIN quality_carbon_new.carbon_landing_T_ProductFeature pf ON m.SubFearureID = pf.SubFeatureID
              LEFT JOIN quality_carbon_new.carbon_landing_productinfovr vr on vr.productinfovrid = m.releaseid
              LEFT JOIN quality_carbon_new.carbon_landing_productline pl on pl.productlineid = vr.productlineid
where (t.NodeCode=1 or t.NodeCode=3);

truncate table quality_outbound_hive.outbound_defect_category;
truncate table quality_outbound_hive.outbound_defect_test_tools;

"

