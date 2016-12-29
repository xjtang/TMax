#!/bin/bash
# Submit multiple jobs to run gw_qsub.sh

echo 'Submit job' $1 'to' $2
for i in $(seq $1 $2); do
    echo 'Submitting job no.' $i
    chmod u+x ./gw_qsub.sh
    qsub ./gw_qsub.sh $i
done

# end
