#!/bin/bash

#1,新建事实表，并导入事实表的全量数据。
#2,新建维度表。
#pwd!@#

sparrow --master yarn --num-executors 8  --executor-cores 4 --executor-memory 8g --driver-memory 8g --driver-cores 4 -e "
create database if not exists quality_carbon_new;

truncate table quality_landing_hive.richtext_all_std;
insert into quality_landing_hive.richtext_all_std select richtextid,content,creationdate,creator,modificationdate,modifier,deleteflag,operation_type,lastupdate_timestamp from quality_landing_hive.richtext_all;
truncate table quality_landing_hive.richtext_stg;
insert into quality_landing_hive.richtext_stg select * from quality_landing_hive.richtext_all_std;


--创建事实表，并导入数据。

create table if not exists quality_carbon_new.carbon_landing_defectmain(DefectID string,
DefectNO string,
CopyFrom string,
SignificantInheritID string,
IsSignificant string,
CriticalDefectInterfaceManID string,
NotesLink string,
ReleaseID string,
BaseLineID string,
SubFearureID string,
ModuleVersionID string,
FindPeriod string,
ODCActivity string,
ODCTrigger string,
ODCSeverity string,
ODCImpact string,
FindDegree string,
EmergentDegree string,
Repeatable string,
TestNO string,
ResolvePRI string,
IfLeak string,
LeakReason string,
ODCTargetLittleLevel string,
ODCDefectType string,
ODCDefectContentType string,
DutyDeptID string,
Team string,
ODCDefectQualifier string,
SourceDefectNO string,
ODC_Source string,
ODCAge string,
BaselineLeadinto string,
CauseReason string,
NeedSync string,
IfPartFailure string,
PartFailureReason string,
PartFailureType string,
IfPartRelated string,
PartID string,
PartBatch string,
PartQuantity string,
SolutionVerifier string,
ModifyVerifier string,
DefactModifier string,
Summery string,
OriginType string,
QResolveVersion string,
SubmitBy string,
SubmitDate timestamp,
DiscoveryDate timestamp,
IsDuplicated string,
DupLink string,
ReportQuality string,
CheckInMergeReleaseID string,
CheckInMergeBaseLineID string,
PlanMergeReleaseID string,
PlanMergeBaseLineID string,
TestReleaseID string,
TestBaseLineID string,
LastResult string,
LastModificationDate timestamp,
CreationDate timestamp,
Creator string,
ModificationDate timestamp,
Modifier string,
DeleteFlag int,
DefectType string,
HPAlmID string,
HPStatus string,
HPCommentID string,
IfAffectVersion string,
IfISSU string,
FeatureChangeType string,
If_Transplant string,
TransplantReason string,
BatchCopyFromNo string,
OtherReason string,
RequiredSolveDate timestamp,
ProgressRemark string,
operation_type string,
lastupdate_timestamp timestamp) 
stored as parquet;
truncate table quality_carbon_new.carbon_landing_defectmain;
insert into table quality_carbon_new.carbon_landing_defectmain select DefectID,DefectNO,CopyFrom,SignificantInheritID ,IsSignificant ,CriticalDefectInterfaceManID ,NotesLink ,ReleaseID ,BaseLineID ,SubFearureID ,ModuleVersionID ,FindPeriod ,ODCActivity ,ODCTrigger ,ODCSeverity ,ODCImpact ,FindDegree ,EmergentDegree ,Repeatable ,TestNO ,ResolvePRI ,IfLeak ,LeakReason ,ODCTargetLittleLevel ,ODCDefectType ,ODCDefectContentType ,DutyDeptID ,Team ,ODCDefectQualifier ,SourceDefectNO ,ODC_Source ,ODCAge ,BaselineLeadinto ,CauseReason ,NeedSync ,IfPartFailure ,PartFailureReason ,PartFailureType ,IfPartRelated ,PartID ,PartBatch ,PartQuantity ,SolutionVerifier ,ModifyVerifier ,DefactModifier ,Summery ,OriginType ,QResolveVersion ,SubmitBy ,SubmitDate ,DiscoveryDate ,IsDuplicated ,DupLink ,ReportQuality ,CheckInMergeReleaseID ,CheckInMergeBaseLineID ,PlanMergeReleaseID ,PlanMergeBaseLineID ,TestReleaseID ,TestBaseLineID ,LastResult ,LastModificationDate ,CreationDate ,Creator ,ModificationDate ,Modifier ,DeleteFlag int,DefectType ,HPAlmID ,HPStatus ,HPCommentID ,IfAffectVersion ,IfISSU ,FeatureChangeType ,If_Transplant ,TransplantReason ,BatchCopyFromNo ,OtherReason ,RequiredSolveDate ,ProgressRemark ,operation_type ,lastupdate_timestamp from quality_landing_hive.defectmain_all;


--richtext表的content字段太宽，需要特殊处理。

create table if not exists quality_carbon_new.carbon_landing_richtext(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet
tblproperties(no_inverted_index='richtextid,content,creationdate,modificationdate', 
sort_columns='creator');
truncate table quality_carbon_new.carbon_landing_richtext;

insert into table quality_carbon_new.carbon_landing_richtext select richtextid,content,creationdate,creator,modificationdate,modifier,deleteflag,operation_type,lastupdate_timestamp from quality_landing_hive.richtext_all_std where lengthB(content) < 32000;

insert into table quality_carbon_new.carbon_landing_richtext select richtextid,substring(content,0,10000),creationdate,creator,modificationdate,modifier,deleteflag,operation_type,lastupdate_timestamp from quality_landing_hive.richtext_all_std where lengthB(content) >= 32000;


create table if not exists quality_carbon_new.carbon_landing_defectprocessflow(defectprocessflowid string,
defectid string,
workflowinstanceid string,
oldtracknumber string,
reader string,
currentperson string,
currentpersonkind string,
currentnode string,
status string,
suspendreason string,
lastprocessed timestamp,
momo string,
submittodevdate timestamp,
testdate timestamp,
testpassed string,
closedcycle int,
testfailedtimes int,
testerreturnreason string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
tmapprovetime timestamp,
positiontime timestamp,
pmapprovetime timestamp,
designtime timestamp,
solutionreviewtime timestamp,
cmoauthorizedtime timestamp,
defectmodifiedtime timestamp,
modifiedreviewtime timestamp,
cmoarchivetime timestamp,
testarrangedtime timestamp,
testtime timestamp,
confirmtime timestamp,
modifyauthorizedby string,
archiveby string,
testmanager string,
testby string,
modifyfilesid string,
currentpersonextra string,
readers string,
synchronousgroupname string,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
truncate table quality_carbon_new.carbon_landing_defectprocessflow;
insert into table quality_carbon_new.carbon_landing_defectprocessflow select * from quality_landing_hive.defectprocessflow_all;

create table if not exists quality_carbon_new.carbon_landing_workflowrecord(workflowrecordid string,
workflowtaskid string,
remarks string,
comments string,
workflowtransitionid string,
cc string,
ccgroup string,
ccall string,
timeapproved timestamp,
approver string,
nextapprover string,
version string,
status string,
txtext1 string,
txtext2 string,
txtext3 string,
txtext4 string,
txtext5 string,
txtext6 string,
txtext7 string,
dateext1 timestamp,
rtxtext1 string,
rtxtext2 string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
truncate table quality_carbon_new.carbon_landing_workflowrecord;
insert into table quality_carbon_new.carbon_landing_workflowrecord select * from quality_landing_hive.workflowrecord_all;

create table if not exists quality_carbon_new.carbon_landing_workflowtask(workflowtaskid string,
workflowinstanceid string,
nodename string,
nodecode string,
owner string,
currentownerkind string,
previoustaskid string,
state string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
truncate table quality_carbon_new.carbon_landing_workflowtask;
insert into table quality_carbon_new.carbon_landing_workflowtask select * from quality_landing_hive.workflowtask_all;



--创建维度表
create table if not exists quality_carbon_new.carbon_landing_productinfobd(
ProductInfoBDID string,
ProductInfoVRID string,
Name string,
Code string,
PlatformVersion string,
PublicName string,
ParentCode string,
CreationDate timestamp,
Creator string,
ModificationDate timestamp,
Modifier string,
DeleteFlag int, 
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
insert into quality_carbon_new.carbon_landing_productinfobd select * from quality_landing_hive.productinfobd_all;

create table  if not exists quality_carbon_new.carbon_landing_productinfovr(
ProductInfoVRID string,
ProductLineID string,
ProductCode string,
ProductName string,
VCode string,
VName string,
RCode string,
RName string,
PDTCode string,
CreationDate timestamp,
Creator string,
ModificationDate timestamp,
Modifier string,
DeleteFlag int,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
insert into quality_carbon_new.carbon_landing_productinfovr select * from quality_landing_hive.productinfovr_all;

create table if not exists quality_carbon_new.carbon_landing_productline(
ProductLineID string,
IPMTCode string,
IPMTName string,
ProductLineCode string,
ProductLineName string,
CreationDate string,
Creator string,
ModificationDate timestamp,
Modifier string,
DeleteFlag int,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
insert into quality_carbon_new.carbon_landing_productline select * from quality_landing_hive.productline_all;

CREATE TABLE if not exists quality_carbon_new.carbon_landing_appconstant
(DefectFlowConstantID string, 
Code string, 
NotesCode string, 
Name string, 
ParentID string, 
Status string, 
Memo string, 
DisplayOrder string, 
Level INT, 
IsEnum string, 
CreationDate  timestamp, 
Creator string, 
ModificationDate  timestamp, 
Modifier string,
DeleteFlag INT,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
insert into quality_carbon_new.carbon_landing_appconstant select * from quality_landing_hive.appconstant_all;

CREATE TABLE if not exists quality_carbon_new.carbon_landing_T_ProductFeature(
ProductFeatureID string,
ProductInfoVRID string,
FeatureID string,
SubFeatureID string,
FeatureCode string,
FeatureName string,
SubFeatureCode string,
SubFeatureName string,
FeatureLevel int,
DeleteFlag int,
LanguageKey string,
operation_type string,
lastupdate_timestamp timestamp)
stored as parquet;
insert into quality_carbon_new.carbon_landing_T_ProductFeature select * from quality_landing_hive.t_productfeature_all;

"

exit 0
