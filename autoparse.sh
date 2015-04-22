#!/bin/bash

SOURCE_DIR=/mnt/light/dns-registration
RAM_DRIVE_DIR=/data/untarplane
INDEX_FILE=index_file.txt
PARSE_SCRIPT_DIR=$RAM_DRIVE_DIR/zonefile

make
mkdir -p $PARSE_SCRIPT_DIR
cp -u zonefile_parse $PARSE_SCRIPT_DIR

for date in `cat $INDEX_FILE`; do
	echo "Copying $SOURCE_DIR/$date to $RAM_DRIVE_DIR"
	cp -r $SOURCE_DIR/$date $RAM_DRIVE_DIR
	for zipfile in $RAM_DRIVE_DIR/$date/*; do
		{ #try
			echo "Unzipping $zipfile" 	&&
			gunzip $zipfile				&&
			filename="${zipfile%.*}"	&&
			echo "Parsing $filename"	&&
			$PARSE_SCRIPT_DIR/zonefile_parse $filename $date
		} ||
		{ #catch
			echo -e "\033[0;31m[FAILED]\t$zipfile\033[0m"
		}
	done
	echo "Removing $RAM_DRIVE_DIR/$date"
	rm -rf $RAM_DRIVE_DIR/$date
done


