#!/bin/bash

USAGE="Usage: ./run.sh -a ALGORITHM -s SORT -m MAPPERS -r REDUCERS [-n LINES]\n\tALGORITHM: page (page edits) | user (user edits)\n\tSORT: id (page/user ID) | edits (number of edits) | no (don't sort)"
LINES=5
NAME=""
INPUT=input/input.txt
OUTPUT=output/
LOCAL_OUTPUT=output/output.txt
MAPPERS=1
REDUCERS=1

if [ $# -lt 8 -o "$1" != "-a" -o "$3" != "-s" -o "$5" != "-m" -o "$7" != "-r" ]
then
	echo "Bad parameters"
	echo -e $USAGE
	exit
fi

if [ "$2" != page -a "$2" != user ]
then
	echo "Invalid algorithm"
	echo -e $USAGE
	exit
fi

if [ "$4" != id -a "$4" != edits -a "$4" != no ]
then
	echo "Invalid sort"
	echo -e $USAGE
	exit
fi

if [ $# -gt 8 -a $# -ne 10 ]
then
	echo "Invalid number of parameters"
	echo -e $USAGE
	exit
fi

if [ $# -eq 10 ]
then
	if [ "$9" != "-n" ]
	then
		echo "Bad parameters"
		echo -e $USAGE
		exit
	fi
fi

if [ "$2" == "page" ]
then
	NAME="WikiPageEditPage"
else
	NAME="WikiPageEditUser"
fi

MAPPERS=$6
REDUCERS=$8

if [ "$4" == id ]
then
	REDUCERS=1
fi

time hadoop jar $NAME.jar org.myorg.$NAME $INPUT $OUTPUT $MAPPERS $REDUCERS $4

if [ "$4" == no -o "$4" == id ]
then
	time hadoop dfs -getmerge output output/output.txt
else
	time hadoop dfs -getmerge output/2 output/output.txt
fi
