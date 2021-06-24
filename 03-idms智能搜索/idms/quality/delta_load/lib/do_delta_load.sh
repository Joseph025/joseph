#!/bin/bash
# 1-incr 2-tmp 3-carbon
do_delta_load(){
num=$(spark-sql -e "select count(*) from $1 " | cut -f 1)
#num = $(sparrow --master yarn --num-executors 8  --executor-cores 4 --executor-memory 32g --driver-memory 8g --driver-cores 4 --conf spark.driver.maxResultSize=0 -e "select count(*) from $1 limit 10" | cut -f 1)
if [ $num -gt 0 ]
then
echo $(date) ':' $1' row count is '$num'.Start process..'
sparrow --master yarn --num-executors 4  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 --conf spark.driver.maxResultSize=0 -e  "
truncate table $2;
insert into table $2 select * from $3 
where $4 in(
select $4 from $3
except
select $4 from $1);

truncate table $3;
insert into table $3 select * from $2;
insert into table $3 select * from $1;
"
echo $(date) ':' $1' process finished!'
else 
    echo 'no delta data.'
fi
}


do_defectmain_delta_load(){
num=$(spark-sql -e "select count(*) from $1 " | cut -f 1)
#num = $(sparrow --master yarn --num-executors 8  --executor-cores 4 --executor-memory 32g --driver-memory 8g --driver-cores 4 --conf spark.driver.maxResultSize=0 -e "select count(*) from $1 limit 10" | cut -f 1)
if [ $num -gt 0 ]
then
echo $(date) ':' $1' row count is '$num'.Start process..'
sparrow --master yarn --num-executors 4  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 --conf spark.driver.maxResultSize=0 -e  "
truncate table $2;
insert into table $2 select * from $3 
where $4 in(
select $4 from $3
except
select $4 from $1);

truncate table $3;
insert into table $3 select * from $2;
insert into table $3 select DefectID,DefectNO,CopyFrom,SignificantInheritID ,IsSignificant ,CriticalDefectInterfaceManID ,NotesLink ,ReleaseID ,BaseLineID ,SubFearureID ,ModuleVersionID ,FindPeriod ,ODCActivity ,ODCTrigger ,ODCSeverity ,ODCImpact ,FindDegree ,EmergentDegree ,Repeatable ,TestNO ,ResolvePRI ,IfLeak ,LeakReason ,ODCTargetLittleLevel ,ODCDefectType ,ODCDefectContentType ,DutyDeptID ,Team ,ODCDefectQualifier ,SourceDefectNO ,ODC_Source ,ODCAge ,BaselineLeadinto ,CauseReason ,NeedSync ,IfPartFailure ,PartFailureReason ,PartFailureType ,IfPartRelated ,PartID ,PartBatch ,PartQuantity ,SolutionVerifier ,ModifyVerifier ,DefactModifier ,Summery ,OriginType ,QResolveVersion ,SubmitBy ,SubmitDate ,DiscoveryDate ,IsDuplicated ,DupLink ,ReportQuality ,CheckInMergeReleaseID ,CheckInMergeBaseLineID ,PlanMergeReleaseID ,PlanMergeBaseLineID ,TestReleaseID ,TestBaseLineID ,LastResult ,LastModificationDate ,CreationDate ,Creator ,ModificationDate ,Modifier ,DeleteFlag int,DefectType ,HPAlmID ,HPStatus ,HPCommentID ,IfAffectVersion ,IfISSU ,FeatureChangeType ,If_Transplant ,TransplantReason ,BatchCopyFromNo ,OtherReason ,RequiredSolveDate ,ProgressRemark ,operation_type ,lastupdate_timestamp from $1
"
echo $(date) ':' $1' process finished!'
else
    echo 'no delta data.'
fi
}



do_richtext_delta_load(){
num=$(spark-sql -e "select count(*) from quality_landing_hive.richtext_incr" | cut -f 1)
if [ $num -gt 0 ]
then
echo $(date) ':' 'quality_landing_hive.richtext_incr row count is'$num'.Start process..'
sparrow --master yarn --num-executors 4  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 --conf spark.driver.maxResultSize=0 -e "
insert into quality_landing_hive.richtext_all_std select richtextid,htmlparser(regexp_replace(content, '(&nbsp\;)+', ' ')),creationdate,creator,modificationdate,modifier,deleteflag,operation_type,lastupdate_timestamp from quality_landing_hive.richtext_incr;

truncate table quality_landing_hive.richtext_stg;

insert into quality_landing_hive.richtext_stg
SELECT A.* FROM quality_landing_hive.richtext_all_std A
INNER JOIN (
 SELECT richtextid,MAX(lastupdate_timestamp) AS lastupdate_timestamp 
FROM quality_landing_hive.richtext_all_std 
GROUP BY richtextid
) B 
ON A.lastupdate_timestamp = B.lastupdate_timestamp AND A.richtextid= B.richtextid;

truncate table quality_carbon_new.carbon_landing_richtext;
insert into table quality_carbon_new.carbon_landing_richtext select * from quality_landing_hive.richtext_stg where lengthB(content) < 32000;
insert into table quality_carbon_new.carbon_landing_richtext select richtextid,substring(content,0,10000),creationdate,creator,modificationdate,modifier,deleteflag,operation_type,lastupdate_timestamp from quality_landing_hive.richtext_stg where lengthB(content) >= 32000;
"
echo $(date) ':' 'quality_landing_hive.richtext_incr process finished!'
else
	echo $(date)':No delta data in richtext.'

fi
}

do_dimension_refresh(){
echo "refreshing dimension table"
sparrow --master yarn --num-executors 8  --executor-cores 4 --executor-memory 32g --driver-memory 8g --driver-cores 4 --conf spark.driver.maxResultSize=0 -e "
truncate table quality_carbon_new.carbon_landing_productline;
insert into table quality_carbon_new.carbon_landing_productline 
select * from quality_landing_hive.productline_all;


truncate table quality_carbon_new.carbon_landing_productinfobd;
insert into table quality_carbon_new.carbon_landing_productinfobd 
select * from quality_landing_hive.productinfobd_all;

truncate table quality_carbon_new.carbon_landing_productinfovr;
insert into table quality_carbon_new.carbon_landing_productinfovr
select * from  quality_landing_hive.productinfovr_all; 

truncate table quality_carbon_new.carbon_landing_appconstant ;
insert into table quality_carbon_new.carbon_landing_appconstant 
select * from quality_landing_hive.appconstant_all;

truncate table quality_carbon_new.carbon_landing_T_ProductFeature;
insert into table quality_carbon_new.carbon_landing_T_ProductFeature
select * from quality_landing_hive.T_ProductFeature_all;
"
}


