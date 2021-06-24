#!/bin/bash
export DELTA_HOME=/usr/local/quality/delta_load
source $DELTA_HOME/lib/do_delta_load.sh
do_defectmain_delta_load quality_landing_hive.defectmain_incr quality_carbon_new.carbon_landing_defectmain_tmp quality_carbon_new.carbon_landing_defectmain defectid
do_delta_load quality_landing_hive.defectprocessflow_incr quality_carbon_new.carbon_landing_defectprocessflow_tmp quality_carbon_new.carbon_landing_defectprocessflow defectprocessflowid
do_delta_load quality_landing_hive.workflowtask_incr quality_carbon_new.carbon_landing_workflowtask_tmp quality_carbon_new.carbon_landing_workflowtask workflowtaskid
do_delta_load quality_landing_hive.workflowrecord_incr quality_carbon_new.carbon_landing_workflowrecord_tmp quality_carbon_new.carbon_landing_workflowrecord workflowrecordid
do_richtext_delta_load
do_dimension_refresh
