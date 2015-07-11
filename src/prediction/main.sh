#!/bin/bash

estfmt() {
    case $1 in
	classifier) m=classification ;;
	estimator) m=estimation ;;
	*)
	    echo "Error"
	    exit 1
	    ;;
    esac

    e=--$1
    est=( $e `echo ${@:2} | sed -e"s/ / $e /g"` --model $m )
    echo ${est[@]}

    return
}

path=`cd $(dirname $0) && pwd`

unset header
machines=(
    # svm
    bayes
    # forest
    # tree
)
mtype=classifier
out=$HOME/data/traffic/log/prediction/`date +%Y-%m%d-%H%M%S`

mkdir --parents $out
( cd log; rm --force current; ln --symbolic `basename $out` current )
cp $0 $out

for pw in 6; do # `seq 3 3 6`; do
    for n in 1; do # `seq 0 1`; do
	echo "[ `date` ] $pw $n" >> $out/trace
	    
	python3 $path/main.py \
	    --neighbors $n \
	    --observation-window 10 \
	    --prediction-window $pw \
	    --target-window 5 \
	    --speed-threshold -0.002 \
	    --${header}print-header \
	    --k-folds 12 \
	    `estfmt $mtype ${machines[@]}`
	
	if [ $? -eq 0 ]; then
	    header=no-
	fi
    done
done > $out/dat 2> $out/log

#
# Neighbor readings are interpolated and the same feature vector is
# used in the neighbor and non-neighbor case
#
