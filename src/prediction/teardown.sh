#!/bin/bash

for i in `find prediction -type d -name 'observations'`; do
    tar -caf $i.tar.xz -C `dirname $i` `basename $i` &&
        rm --recursive --force $path/observations
done
