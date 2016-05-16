#!/bin/bash

toopts() {
    opt=--$1
    shift
    if [ $# -gt 0 ]; then
	echo "$opt `sed -e"s/ / $opt /g" <<< $@`"
    fi
    
    return
}

while getopts "l:f:d:h" OPTION; do
    case $OPTION in
	l) logs=( ${logs[@]} $OPTARG ) ;;
	f) filters=( ${filters[@]} $OPTARG ) ;;
	d) display="--display $OPTARG" ;;
	h)
	    cat <<EOF
$0 [options]
 -l log directory
 -f filters
 -d display

Log directory and filters can be specified multiple times; all filters
will be applied to all log directories.

Display should be either 'monitor' or 'presentation', depending on
where the generated figures will be consumed.
EOF
	    exit
	    ;;
	*) exit 1 ;;
    esac
done

for i in ${logs[@]}; do
    echo $i

    datfile=$i/dat
    if [ ! -e $datfile -o `stat --format='%s' $datfile` -eq 0 ]; then
	continue
    fi

    fig=$i/fig
    if [ ! -e $fig ]; then
	mkdir $i/fig
    fi

    for j in f1_score matthews_corrcoef; do
	echo $j
	python3 $NYCTRAFFIC/src/prediction/plot.py \
		--data $datfile \
		--plot-directory $fig \
		--cluster-tld $NYCTRAFFICLOG/cluster \
		--metric  $j \
		$display \
		`toopts filter ${filters[@]}` ||
	    exit
    done
done
