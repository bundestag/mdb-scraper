#!/bin/bash

ARCHIVE=$DATA_PATH/bundestag/mdb
POPOLOFILE=$ARCHIVE/popolo-`date +%Y%m%d`.json

mkdir -p $ARCHIVE

aws s3 cp $POPOLOFILE s3://archive.pudo.org/bundestag/mdb/popolo-`date +%Y%m%d`.json
aws s3 cp $POPOLOFILE s3://archive.pudo.org/bundestag/mdb/popolo-latest.json

python scraper.py $POPOLOFILE
