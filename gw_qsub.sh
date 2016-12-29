#!/bin/bash

# Submit qsub job

# Specify which shell to use
#$ -S /bin/bash

# Run for 24 hours
#$ -l h_rt=96:00:00

# Forward my current environment
#$ -V

# Give this job a name
#$ -N cd_batch

# Join standard output and error to a single file
#$ -j y

# Name the file where to redirect standard output and error
#$ -o qsub.qlog

# Now let's keep track of some information just in case anything goes wrong

echo "=========================================================="
echo "Starting on : $(date)"
echo "Running on node : $(hostname)"
echo "Current directory : $(pwd)"
echo "Current job ID : $JOB_ID"
echo "Current job name : $JOB_NAME"
echo "Task index number : $SGE_TASK_ID"
echo "=========================================================="

# Run the bash script
R --slave --vanilla --no-save  <<EEE
source('/projectnb/landsat/users/xjtang/global_warming/script/process_GHCN.R')
tmax_GHCN('/projectnb/landsat/users/xjtang/global_warming/stage4/','/projectnb/landsat/users/xjtang/global_warming/files/both.csv','/projectnb/landsat/users/xjtang/global_warming/result/',$1)
EEE

echo "=========================================================="
echo "Finished on : $(date)"
echo "=="
