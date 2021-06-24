#!/bin/bash

sparrow --master yarn --num-executors 12  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 -e "

insert into table quality_carbon.es_idms_defect_v2 SELECT t3.OWNER AS OWNER
         ,vr.Rname AS Rname
         ,vr.Vname AS Vname 
         ,nvl(rai1.attached_file_num,0) AS att_file_num1
         ,nvl(rai3.attached_file_num,0) AS att_file_num3		 
         ,nvl(rai1.attached_img_num,0) AS att_img_num1  
         ,nvl(rai3.attached_img_num,0) AS att_img_num3 
         ,bd.NAME AS baseline
         ,split(nvl(dc.categories,''), ',') AS category
         ,nvl(dc.categories,'') as categoryStr
         ,rt.Content AS content
         ,dm.CreationDate AS creationdate
         ,t2.CurrentNode AS currentNode
         ,t2.CurrentPerson AS currentPerson
         ,split(nvl(xwc.words,''),',') AS cut_words
         ,dm.DefectID AS defectID
         ,dm.SubmitBy AS defectModifier
         ,dm.DefectNO AS defectNo
         ,a1.NAME AS defect_ODCSeverity
         ,t2.LastProcessed AS lastProcessed
         ,dm.lastupdate_timestamp AS lastupdateTimestamp
         ,CASE WHEN t2.CurrentNode = '3' OR t2.CurrentNode = '1' OR t2.CurrentNode = 'Z' OR t2.LastProcessed IS NULL OR t2.LastProcessed > current_timestamp() THEN 0 ELSE datediff(current_timestamp(),t2.LastProcessed) END  as lengthofstay
         ,t3.NodeCode AS nodeCode
         ,t3.NodeName AS nodeName
         ,dm.operation_type AS operation_type
         ,pl.ProductLineName AS productLineName
         ,vr.ProductName AS productName
         ,current_timestamp() as refresh_timestamp
         ,t2.status AS status
         ,dm.DefactModifier AS submitBy
         ,dm.SubmitDate AS submitDate
         ,dm.Summery AS summary
         ,t2.suspendreason  AS suspendreason
         ,split(nvl(dt.test_tools,''), ',') as testTool
         ,nvl(dt.test_tools,'') as testToolStr  
FROM  quality_carbon.carbon_landing_defectmain AS dm
LEFT JOIN  quality_carbon.carbon_landing_productinfovr AS vr ON dm.releaseid = vr.productinfovrid
LEFT JOIN  quality_carbon.carbon_landing_productline AS pl ON vr.ProductLineID = pl.ProductLineID
LEFT JOIN  quality_carbon.carbon_landing_defectprocessflow t2 ON dm.DefectID = t2.DefectID
LEFT JOIN  quality_carbon.carbon_landing_workflowtask t3 ON t3.WorkflowInstanceID = t2.WorkflowInstanceID
         AND (
                   t3.NodeCode = '1'
                   AND t3.PreviousTaskID = '00000000-0000-0000-0000-000000000000'
                   )
LEFT JOIN  quality_carbon.carbon_landing_workflowrecord t4 ON t4.WorkflowTaskID = t3.WorkflowTaskID
LEFT JOIN  quality_landing_hive.richtext_stg AS rt ON t4.Comments = rt.RichTextId
LEFT JOIN  quality_carbon.carbon_landing_productinfobd AS bd ON dm.baselineid = bd.productinfobdid
LEFT JOIN  quality_carbon.carbon_landing_appconstant AS a1 ON dm.ODCSeverity = a1.Code
LEFT JOIN  quality_outbound_hive.outbound_defect_category AS dc on dm.defectID = dc.defectID
LEFT JOIN  quality_outbound_hive.outbound_defect_test_tools AS dt on dm.defectID=dt.defectID 
LEFT JOIN ( 
SELECT WorkflowInstanceID,WorkflowTaskID,
ROW_NUMBER() OVER(PARTITION BY WorkflowInstanceID,NodeCode ORDER BY ModificationDate DESC) AS RN 
FROM quality_carbon.carbon_landing_workflowTask 
WHERE NodeCode = '3' 
) t3_3 ON t3_3.WorkflowInstanceID = t2.WorkflowInstanceID AND t3_3.RN = 1 
LEFT JOIN  quality_carbon.carbon_landing_workflowrecord t4_3 ON t4_3.WorkflowTaskID = t3_3.WorkflowTaskID
LEFT JOIN  quality_outbound_hive.richtext_attached_info AS rai1 on rai1.richtextid = t4.Comments 
LEFT JOIN  quality_outbound_hive.richtext_attached_info AS rai3 on rai3.richtextid = t4_3.Comments 
LEFT JOIN  quality_outbound_hive.idms_x_word_cut AS xwc on xwc.defectid = dm.DefectID;"