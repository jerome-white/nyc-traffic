#!/bin/bash

mkcurrent() {
    cd $1
    rm --force current
    ln --symbolic `basename $out` current
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
( mkcurrent `dirname $out` )

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
# Classification variables: require run iterations
#

transform=(
    simple
    # change
    # average
    # difference
)
neighbor_selection=(
    simple
    # var
    # hybrid
)
observation_window=( 12 )
prediction_window=( 3 )
target_window=( 3 )
depth=( `seq 0 8` )

unset opts
for a in ${observation_window[@]}; do # 0
    for b in ${prediction_window[@]}; do # 1
	for c in ${target_window[@]}; do # 2
	    for d in ${depth[@]}; do # 3
		for e in ${transform[@]}; do # 4
		    for f in ${neighbor_selection[@]}; do # 5
			args=$a:$b:$c:$d:$e:$f
			opts=( ${opts[@]} $args )
		    done
		done
	    done
	done
    done
done

#
# Prediction variables: can be run in the same classification step.
#

machines=(
    # svm
    # bayes
    forest
    # tree
    dummy
)

#
# Begin the run!
#
i=1
pheader=True
config=$NYCTRAFFICOPTS/prediction.ini

for o in ${opts[@]}; do
    args=( `sed -e's/:/ /g' <<< $o` )
    echo "[ `date` : $i/${#opts[@]} ] ${args[@]}" >> $out/trace
    
    if [ $i -gt 1 ]; then
	pheader=False
    fi

    cat <<EOF > $config
[window]
observation=${args[0]}
prediction=${args[1]}
target=${args[2]}

[machine]
folds=10
method=`sed -e's/ /,/g' <<< ${machines[@]}`
model=classification
feature-transform=${args[4]}

[neighbors]
depth=${args[3]}
selection=${args[5]}

[output]
print-header=$pheader

[parameters]
acceleration=-0.002
intra-reporting=120
EOF
	    
    python3 $NYCTRAFFIC/src/prediction/main.py --config $config
    (( i++ ))
done > $out/dat 2> $out/log
