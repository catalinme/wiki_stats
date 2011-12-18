#!/bin/bash

if [ $# -lt 1 ]
then
	echo "Usage: /get_page_name id"
fi

page_name=`sed -n ''$1','$1'p' titles-sorted.txt`

echo "Page with id $1 is $page_name"
