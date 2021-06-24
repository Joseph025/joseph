#!/bin/bash
#pwd!@#
sparrow --master yarn --num-executors 12  --executor-cores 4 --executor-memory 16g --driver-memory 8g --driver-cores 4 -e "

CREATE TABLE IF NOT EXISTS quality_carbon_new.es_idms_defect_v4
USING org.elasticsearch.spark.sql
OPTIONS
( 
resource 'idms_defect_v4',
nodes '10.64.8.51',
pushdown 'true',
port '9200',
es.read.field.as.array.include 'category,testTool,cut_words',
es.mapping.id 'defectID',
es.net.http.auth.user='es7prodadmin'
);
"