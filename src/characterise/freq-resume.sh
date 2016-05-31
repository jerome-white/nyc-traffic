#!/bin/bash

if [ ! -e $1/observations.pkl ]; then
    exit 1
fi

python3 $NYCTRAFFIC/src/characterise/frequency.py \
        --figure-directory $1 \
        --resume $1/observations.pkl
