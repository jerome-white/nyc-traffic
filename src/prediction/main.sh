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

unset header
machines=(
    # svm
    # bayes
    forest
    # tree
)
mtype=classifier
nycpath=$NYCTRAFFIC/src/prediction
out=$nycpath/log/`date +%Y_%m-%d_%H%M`.`hostname`
if [ -e $out ]; then
    echo "$out exists" 1>&2
    exit 1
fi

mkdir --parents $out
( cd log; rm --force current; ln --symbolic `basename $out` current )
cp $0 $out
cp $NYCTRAFFIC/etc/opts/prediction $out

for pwindow in 6; do
    for neighbors in 1; do
	for cluster in simple var; do
	    echo "[ `date` ] $pw $n" >> $out/trace
	    
	    python3 $nycpath/main.py \
		--neighbors $neighbors \
		--neighbor-selection $cluster \
		--observation-window 10 \
		--prediction-window $pwindow \
		--target-window 5 \
		--speed-threshold -0.002 \
		--${header}print-header \
		--k-folds 10 \
		--aggregator simple \
		`estfmt $mtype ${machines[@]}`
	
	    if [ $? -eq 0 ]; then
		header=no-
	    fi
	done
    done
done > $out/dat 2> $out/log
