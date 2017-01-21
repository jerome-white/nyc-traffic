#!/bin/bash

for i in `find $1 -type d -name 'observations'`; do
    echo "[ `date` ] $i"
    j=`dirname $i`
    tar --create --xz --file=$i.tar.xz --directory=$j `basename $i` &&
        rm --recursive --force $path/observations
done
