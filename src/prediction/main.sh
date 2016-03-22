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
    sleep 60
done

#
# Begin the run!
#
tmp=`mktemp --directory`
$phome/mkconfigs.py \
    --reporting-threshold 120 \
    --output-directory $tmp \
    --parallel \
    --verbose
i=1
for i in $tmp/*; do
    echo "[ `date` : $i ] $REPLY" >> $out/trace
    python3 $phome/main.py --config $i
    (( i++ ))
done > $out/dat 2> $out/log
