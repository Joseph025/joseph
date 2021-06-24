#!/bin/bash
#./2.Init-carbon-table.sh
#./3.Prepare_ml_input_data.sh
#./4.Merge_ml_out_data_to_Carbon.sh  
#./5.Load-data-to-es.sh

echo "########Start initialzie hive table###############"
./1.Init-hive-table.sh
echo "########End initialzie hive table###############"

echo "########Start initialzie carbon table###############"
./2.Init-carbon-table.sh
echo "########End initialzie carbon table###############"

echo "########Start prepare data for ml###############"
./3.Prepare_ml_input_data.sh
echo "########End prepare data for ml###############"

echo "########Start prepare data for ml###############"
./4.Cal_defect_category_and_test_tools.sh
echo "########End prepare data for ml###############"

echo "########Start merge ml data back to carbon###############"
./5.Merge_ml_out_data_to_Carbon.sh
echo "########End merge ml data back to carbon###############"

echo "########Start idms_create_tmp###############"
./6.idms_create_tmp.sh
echo "########End idms_create_tmp###############"

echo "########Start idms_create_es_v4###############"
./7.idms_create_es_v4.sh
echo "########End idms_create_es_v4###############"

echo "########Start idms_insert_to_es_v4###############"
./8.idms_insert_to_es_v4.sh
echo "########End idms_insert_to_es_v4###############"


