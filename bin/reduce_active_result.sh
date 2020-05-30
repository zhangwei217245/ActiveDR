#!/bin/bash

conda activate ActiveDR_env

date_array=( $( cat /global/homes/w/wzhang5/software/ActiveDR/bin/2016_dir.txt ) )

for line in "${date_array[@]}"; do
    echo "working on $line"
    python -u user_activity_analyzer.py -d $line -f reducer
done