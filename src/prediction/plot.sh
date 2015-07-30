#!/bin/bash

toopts() {
    opt=--$1
    shift
    echo "$opt `sed -e"s/ / $opt /g" <<< $@`"
    return
}

traffic=$NYCTRAFFIC/src
while getopts "l:f:h" OPTION; do
    case $OPTION in
	l) logs=( ${logs[@]} $OPTARG ) ;;
	f) filters=( ${filters[@]} $OPTARG ) ;;
	h)
	    cat <<EOF
$0 [options]
 -l log directory
 -f filters

Both can be specified multiple times; all filters will be applied to
all log directories.
EOF
	    exit
	    ;;
	*) exit 1 ;;
    esac
done

prediction=$traffic/prediction
for i in ${logs[@]}; do
    echo $i
    dat=$prediction/$i/dat

    cls=( `ls $traffic/cluster/log/${l[1]}/dat-* 2> /dev/null` )
    if [ ! $cls ]; then
	continue
    fi
    output=`dirname $dat`/fig/$fig
    mkdir --parents $output

    python3 $prediction/plot.py \
	    --data $dat \
	    --stats-file $output/stats.csv \
	    --plot-directory $output \
	    `toopts clusters ${cls[@]}` \
	    `toopts filter ${filters[@]}` || { \
	rm --recursive --force $output
	break
    }
done
