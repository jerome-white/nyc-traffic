#!/bin/bash

ini=ini-pool
p_home=$NYCTRAFFIC/src/prediction
p_log=$NYCTRAFFICLOG/prediction
while getopts "r:n:b:" OPTION; do
    case $OPTION in
        r) readings=$OPTARG ;;
        n) network=$OPTARG ;;
        b)
            if [ -d $OPTARG/$ini ]; then
                base=$OPTARG
            fi
            ;;
        *) exit 1 ;;
    esac
done
mkdir --parents $p_log

if [ ! $base ]; then
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
    # Create the configuration files
    #
    python3 $p_home/mkconfigs.py \
            --reporting-threshold 90 \
            --output-directory $base/$ini
fi

i=1
configs=( `ls $base/$ini/*.ini` )
for j in ${configs[@]}; do
    echo "[ `date` : $i / ${#configs[@]} ] $j" >> $base/trace

    path=$base/`basename $j .ini`
    rm --recursive --force $path
    mkdir --parents $path/{observations,results}
    (cat $j; cat <<EOF) > $path/ini
[data]
raw = $readings
network = $network
results = $path/results
observations = $path/observations
EOF
    
    python3 $p_home/prediction.py --config $path/ini || exit 1
    rm $j
    # tar -cjf $path/observations.tar.bz2 -C $path observations
    tar -cf - -C $path observations | \
	lzma --compress --best --stdout > $path/observations.tar
    rm --recursive --force $path/observations
    (( i++ ))
done
rm --recursive --force $base/$ini
