#!/bin/bash
source /etc/profile

run_dir=/opt/apps/idms/idms_defect_complex_service
cd ${run_dir}

#start service
nohup ${ANACONDA_PYTHON}/python optimization-ws-complex.py > log/nohup.log 2>&1 &
