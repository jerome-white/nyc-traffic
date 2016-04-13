#!/bin/bash

mkcurrent() {
    cd $1
    rm --force current
    ln --symbolic `basename $out` current
}

phome=$NYCTRAFFIC/src/prediction

#
# Create the output directory
#
while true; do
    out=$NYCTRAFFICLOG/prediction/`date +%Y_%m-%d_%H%M`.`hostname`
    if [ ! -e $out ]; then
        mkdir --parents $out
        ( mkcurrent `dirname $out` )
        break
    fi
    # sleep until this minute is finished
    sleep `expr 60 - $(expr $(date +%s) % 60)`
done

#
# Begin the run!
#
tmp=`mktemp --directory`
python3 $phome/mkconfigs.py \
    --reporting-threshold 120 \
    --output-directory $out \
    --skeleton $NYCTRAFFIC/etc/opts/prediction.ini \
    --parallel \
    --quiet

i=0
configs=( `ls $tmp/*` )
for j in ${configs[@]}; do
    echo "[ `date` : $i / ${#configs[@]} ] $j " >> $out/trace
    python3 $phome/main.py --config $j
    (( i++ ))
done > $out/dat 2> $out/log
