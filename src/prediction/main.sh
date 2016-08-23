#!/bin/bash

p_home=$NYCTRAFFIC/src/prediction
p_log=$NYCTRAFFICLOG/prediction/test
while getopts "r:n:" OPTION; do
    case $OPTION in
        r) readings=$OPTARG ;;
        n) network=$OPTARG ;;
        *) exit 1 ;;
    esac
done
mkdir --parents $p_log

#
# Create the output directory
#
while true; do
    base=$p_log/`date +%Y_%m-%d_%H%M`.`hostname`
    if [ ! -e $base ]; then
        mkdir --parents $base
        break
    fi
    # sleep until this minute is finished
    sleep `expr 60 - $(expr $(date +%s) % 60)`
done

#
# Begin the run!
#
tmp=`mktemp --directory --tmpdir=$base ini.XXX`
python3 $p_home/mkconfigs.py \
    --reporting-threshold 120 \
    --output-directory $tmp

i=0
configs=( `ls $tmp/*` )
for j in ${configs[@]}; do
    echo "[ `date` : $i / ${#configs[@]} ] $j " >> $base/trace

    d=$base/`basename $j .ini`
    mkdir --parents $d
    for k in observations results; do
        mkdir $d/$k
    done
    (cat $j; cat <<EOF) > $d/ini
[data]
raw = $readings
network = $network
observations = $d/observations
results = $d/results
EOF
    
    python3 $p_home/prediction.py --config $d/ini
    
    (( i++ ))
done
rm --recursive --force $tmp
