#!/bin/bash

toopts() {
    opt=--$1
    shift
    echo "$opt `sed -e"s/ / $opt /g" <<< $@`"
    return
}

while getopts "l:f:" OPTION; do
    case $OPTION in
	l) logs=( ${logs[@]} $OPTARG ) ;;
	f) filters=( ${filters[@]} $OPTARG ) ;;
	*) exit 1 ;;
    esac
done
traffic=$NYCTRAFFIC/src
prediction=$traffic/prediction
cluster=$traffic/cluster

for i in ${logs[@]}; do
    echo $i
    
    l=( `sed -e's/,/ /g' <<< $i` )
    dat=$prediction/log/${l[0]}/dat

    cls=( `ls $cluster/log/${l[1]}/dat-* 2> /dev/null` )
    if [ ! $cls ]; then
	continue
    fi
    output=`dirname $dat`/fig/$k
    mkdir --parents $output

    python3 $prediction/plot.py \
	    --data $dat \
	    --output $output \
	    `toopts clusters ${cls[@]}` \
	    `toopts filter ${filters[@]}` || \
	break
done
