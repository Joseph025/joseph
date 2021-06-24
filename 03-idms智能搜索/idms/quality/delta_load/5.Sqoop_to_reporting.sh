#!/bin/bash

sqoop export --connect 'jdbc:jtds:sqlserver://10.63.18.61:49839/RD_Digital' --driver net.sourceforge.jtds.jdbc.Driver --username H3RDDigital --password pdmtest --table 'dbo.RdClass' --export-dir '/apps/hive/warehouse/quality_outbound_hive.db/outbound_defect_category' --input-fields-terminated-by '\b' --input-lines-terminated-by '\n' --columns "defectId,categories,loaded_timestamp"
