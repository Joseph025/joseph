#!/bin/bash
export DELTA_HOME=/usr/local/quality/delta_load
echo "Start load data"
$DELTA_HOME/1.Load_delta_data_to_carbon.sh 
echo "Start prepare ml data"
$DELTA_HOME/2.Prepare_ml_input_data.sh
echo "Start Calculate"
$DELTA_HOME/3.Delata_cal_defect_category_and_test_tools.sh  
echo "Start Send to ES"
$DELTA_HOME/4.Load_data_to_es.sh
echo "Send to Reporting"
$DELTA_HOME/5.Sqoop_to_reporting.sh
