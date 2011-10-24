#!/bin/bash

if [ $# -eq 0 ]; then
	no_lines=15
else
	no_lines=$1
fi

cat output/part-00000 | sort -nr -k2,2 | head -$no_lines
