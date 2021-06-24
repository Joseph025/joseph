#!/bin/bash
#pwd!@#

sparrow --master yarn --num-executors 12  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 -e "

create table if not exists quality_carbon_new.idms_tmp_rcode( 
OWNER string,
PMAnalysis string,
Rname string,
Vname string,
adminAdvice string,
approverComments string,
att_file_num1 string,
att_file_num3 string,	 
att_img_num1  string,
att_img_num3 string,
baseline string,
category string,
categoryStr string,
causeAnalysis string,	 
content string,
creationdate timestamp,
currentNode string,
currentPerson string,
cut_words string,
defectID string,
defectModifier string,
defectNo string,
defect_ODCSeverity string,
developerComments string,
issueProcessor string,
lastProcessed string,
lastupdateTimestamp timestamp,
lengthofstay string,
nodeCode string,
nodeName string,
operation_type	 string,
productLineName string,
productName string ,
refresh_timestamp timestamp,
solution string,
status string,
submitBy string,
submitDate timestamp,
summary string ,
suspendRreason string,
testReport string,
testTool string,
testToolStr string,
testerComments string)
stored as parquet;"