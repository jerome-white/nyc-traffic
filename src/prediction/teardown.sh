#!/bin/bash

for i in `find prediction -name 'observations'`; do
    path=`dirname $i`
    XZ_OPT=-9 tar -cJf $path/observations.tar.xz -C $path observations
done
