#!/bin/bash

USAGE="Usage: ./run.sh TITLES EDITS ACCESSES OUTPUT MAPPERS REDUCERS\n\tTITLES: file containing the titles of Wiki pages sorted by line number\n\tEDITS: file containing the number of edits for each Wiki page\n\tACCESSES: file containing the number of accesses for each Wiki page"
NAME="EditViewBalance"

if [ $# -ne 6 ]
then
	echo "Invalid number of parameters"
	echo -e $USAGE
	exit
fi

TITLES=$1
EDITS=$2
ACCESSES=$3
OUTPUT=$4
MAPPERS=$5
REDUCERS=$6
LOCAL_OUTPUT=output/output.txt

time hadoop jar $NAME.jar org.myorg.$NAME $TITLES $EDITS $ACCESSES $OUTPUT $MAPPERS $REDUCERS
time hadoop dfs -getmerge $OUTPUT/3 $LOCAL_OUTPUT
