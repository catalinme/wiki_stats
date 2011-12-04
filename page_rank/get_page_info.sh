#!/bin/bash

if [ $# -lt 3 ]
then
	echo "Usage: ./get_page_info.sh page_id output_file page_name_file"
else
	page_name=`sed -n ''$1','$1'p' $3`
	output_line=`sed -n ''$1','$1'p' $2`
	camera_output_line=$(echo $output_line | tr " " "\n")

	for i in $camera_output_line
	do
	    if [[ $i == r* ]]
		then
			page_rank=`echo $i | sed 's/.\(.*\)/\1/'`
			echo Page rank for $page_name is $page_rank
			exit
		fi
	done
fi

