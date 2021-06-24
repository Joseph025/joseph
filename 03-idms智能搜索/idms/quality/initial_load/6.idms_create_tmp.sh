#!/bin/bash
#pwd!@#

sparrow --master yarn --num-executors 12  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 -e "

create table if not exists quality_carbon_new.idms_tmp_v1( 
DefectID         string,
Content_3        string,  
Approver_3       string, 
Content_4        string,  
Content_5        string,  
Content_A        string,
Content_B        string,
Content_C        string,
Content_E        string,
Content_F        string)  
stored as parquet;

truncate table quality_carbon_new.idms_tmp_v1;
INSERT INTO quality_carbon_new.idms_tmp_v1
SELECT  a.DefectID,
		MAX(CASE WHEN NodeCode='3' THEN Content END) AS causeAnalysis,           
        MAX(CASE WHEN NodeCode='3' THEN Approver END) AS issueProcessor,
        MAX(CASE WHEN NodeCode='4' THEN Content END) AS PMAnalysis,
        MAX(CASE WHEN NodeCode='5' THEN Content END) AS solution,
        MAX(CASE WHEN NodeCode='A' THEN Content END) AS adminAdvice,
        MAX(CASE WHEN NodeCode='B' THEN Content END) AS developerComments,
        MAX(CASE WHEN NodeCode='C' THEN Content END) AS approverComments,
        MAX(CASE WHEN NodeCode='E' THEN Content END) AS testerComments,
        MAX(CASE WHEN NodeCode='F' THEN Content END) AS testReport
FROM (SELECT t3.NodeCode,dm.DefectID,
       COALESCE(CONCAT_WS('',COLLECT_SET(t4.Approver))) AS Approver, 
       COALESCE(CONCAT_WS('',COLLECT_SET(rt.Content))) AS Content
FROM quality_carbon_new.carbon_landing_defectmain AS dm
LEFT JOIN quality_carbon_new.carbon_landing_defectprocessflow AS t2 ON dm.DefectID = t2.DefectID
LEFT JOIN quality_carbon_new.carbon_landing_workflowtask AS t3 ON t3.WorkflowInstanceID = t2.WorkflowInstanceID
LEFT JOIN quality_carbon_new.carbon_landing_workflowrecord AS t4 ON t4.WorkflowTaskID = t3.WorkflowTaskID
LEFT JOIN quality_landing_hive.richtext_stg AS rt ON t4.Comments = rt.RichTextId
GROUP BY dm.DefectID,t3.NodeCode ORDER BY t3.NodeCode) AS a
GROUP BY a.DefectID;
"