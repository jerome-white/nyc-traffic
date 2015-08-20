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

mkcurrent() {
    cd $1
    rm --force current
    ln --symbolic `basename $out` current
    cd $OLDPWD
    
    return
}

#
# Create the output directory
#
out=$NYCTRAFFICLOG/prediction/`date +%Y_%m-%d_%H%M`.`hostname`
if [ -e $out ]; then
    echo "$out exists" 1>&2
    exit 1
fi
mkdir --parents $out
mkcurrent `dirname $out`

#
# Copy accounting files to the output directory
#
records=(
    `pwd`/`basename $0`
    $NYCTRAFFIC/etc/opts/prediction
)
for i in ${records[@]}; do
    cp $i $out
done

#
# Begin the run!
#

# set variables
machines=(
    # svm
    # bayes
    forest
    # tree
    # dummy
)
unset header
mtype=classifier

# run!
for pwindow in 6; do
    for neighbors in 1 2; do

	    echo "[ `date` ] $pw $n" >> $out/trace
	    
	    python3 $NYCTRAFFIC/src/prediction/main.py \
		--neighbors $neighbors \
		--neighbor-selection hybrid \
		--observation-window 12 \
		--prediction-window $pwindow \
		--target-window 4 \
		--speed-threshold -0.002 \
		--${header}print-header \
		--k-folds 100 \
		--aggregator simple \
		`estfmt $mtype ${machines[@]}`
	
	    if [ $? -eq 0 ]; then
		header=no-
	    fi

    done
done > $out/dat 2> $out/log
