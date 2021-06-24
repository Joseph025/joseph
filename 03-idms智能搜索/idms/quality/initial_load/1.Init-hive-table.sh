#!/bin/bash

#1,新建全量事实表。
#2,新建维度表。维度表只有全量，且不用备份，每天覆盖。
#3,新建增量表。
#注意：创建的表为外部表。
#pwd!@#

beeline -u "jdbc:hive2://10.165.8.52:10000/" -n testadmin -p "test1qaz@WSX" -e"
create database if not exists quality_landing_hive;
create database if not exists quality_outbound_hive;
--创建全量事实表。
drop table if exists quality_landing_hive.defectmain_all;
create external table if not exists quality_landing_hive.defectmain_all(DefectID string,
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
FeatureDesc string,
ProblemPhenomenon string,
ProblemConditions string,
IfISSU string,
Deliverables string,
FeatureChangeType string,
If_Transplant string,
TransplantReason string,
BatchCopyFromNo string,
OtherReason string,
RequiredSolveDate timestamp,
ProgressRemark string,
operation_type string,
lastupdate_timestamp timestamp) 
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.defectprocessflow_all;
create external table if not exists quality_landing_hive.defectprocessflow_all(defectprocessflowid string,
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;


drop table if exists quality_landing_hive.workflowrecord_all;
create external table if not exists quality_landing_hive.workflowrecord_all(workflowrecordid string,
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;


drop table if exists quality_landing_hive.workflowtask_all;
create external table if not exists quality_landing_hive.workflowtask_all(workflowtaskid string,
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;


drop table if exists quality_landing_hive.richtext_all;
create external table if not exists quality_landing_hive.richtext_all(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;


create table if not exists quality_landing_hive.richtext_all_std(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

create table if not exists quality_landing_hive.richtext_incr_std(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;



--创建维度表
drop table if exists quality_landing_hive.productinfobd_all;
create external table if not exists quality_landing_hive.productinfobd_all(
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.productinfovr_all;
create external table  if not exists quality_landing_hive.productinfovr_all(
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.productline_all;
create external table if not exists quality_landing_hive.productline_all(
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.appconstant_all;
CREATE external TABLE if not exists quality_landing_hive.appconstant_all
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.T_ProductFeature_all;
create external table if not exists quality_landing_hive.T_ProductFeature_all(
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;





--创建增量事实表
drop table if exists quality_landing_hive.defectmain_incr;
create external table if not exists quality_landing_hive.defectmain_incr(DefectID string,
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
FeatureDesc string,
ProblemPhenomenon string,
ProblemConditions string,
IfISSU string,
Deliverables string,
FeatureChangeType string,
If_Transplant string,
TransplantReason string,
BatchCopyFromNo string,
OtherReason string,
RequiredSolveDate timestamp,
ProgressRemark string,
operation_type string,
lastupdate_timestamp timestamp) 
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.richtext_incr;
create external table if not exists quality_landing_hive.richtext_incr(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.defectprocessflow_incr;
create external table if not exists quality_landing_hive.defectprocessflow_incr(defectprocessflowid string,
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;


drop table if exists quality_landing_hive.workflowrecord_incr;
create external table if not exists quality_landing_hive.workflowrecord_incr(workflowrecordid string,
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;


drop table if exists quality_landing_hive.workflowtask_incr;
create external table if not exists quality_landing_hive.workflowtask_incr(workflowtaskid string,
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
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.richtext_incr_std;
create table if not exists quality_landing_hive.richtext_incr_std(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.richtext_all_std;
create table if not exists quality_landing_hive.richtext_all_std(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

drop table if exists quality_landing_hive.richtext_stg;
create table if not exists quality_landing_hive.richtext_stg(richtextid string,
content string,
creationdate timestamp,
creator string,
modificationdate timestamp,
modifier string,
deleteflag int,
operation_type string,
lastupdate_timestamp timestamp)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with SERDEPROPERTIES ('field.delim'='^|~')
stored as textfile;

"

echo "create tables successfully!"
exit 0
