#!/bin/bash

archive() {
    cd `dirname $1`

    f=`basename $1`
    tar --create --bzip2 --file $f.tar.bz $f
    rm --recursive --force $f
}

DATA=$HOME/data/traffic
if [ ! $NYCTRAFFIC ]; then
    source $HOME/.bash_exports
fi

#
# store the new information into the database
#

for i in `find $DATA/nyc -mindepth 1 -type d | sort | head --lines=-1`; do
    find $i -size 0 -delete

    unset err
    for j in $i/*; do
	python3 $NYCTRAFFIC/src/aquire/nyc/store.py --input $j || err=1
    done
    if [ ! $err ]; then
	( archive $i )
    fi
done
# python3 $NYCTRAFFIC/src/aquire/purge-old.py

#
# backup the database
#

options=(
    add-drop-database
    add-drop-table
    complete-insert
    extended-insert
    hex-blob
    routines
    user=social
    log-error=$HOME/log/mysqldump.err
)
opts=( `sed -e's/ / --/g' <<< ${options[@]}` )
fname=$DATA/mysql/`date +'%F'`.bz
mysqldump --${opts[@]} traffic | bzip2 --best > $fname
chmod 444 $fname

#
# Purge old archives and backups
#

old=(
    # nyc:10
    mysql:30
)
for i in ${old[@]}; do
    args=( `sed -e's/:/ /g' <<< $i` )
    find $DATA/${args[0]} -name '*.bz' -mtime +${args[1]} -delete
done
