#!/bin/bash

schemaname=$1
loadtype=$2
datapath=$3
loadbatch=$4
tablename=$5

count=`ps aux|grep "[d]atasync_driver.py $schemaname $loadtype $datapath 1 $tablename" | wc -l`

if [ $count -gt 1 ]
then
        printf "\nNumber of running sessions : "
        printf $count
        printf "\nSession running already\n"
        echo `date`
        printf "\n===================================\n"
else
                printf "\nNo Session Running.... Spinning of new session\n"
                echo `date`
                nohup /usr/bin/python2.7 -u /apps/datasync/scripts/datasync_driver.py $schemaname $loadtype $datapath 1 $tablename >> /apps/datasync/logs/$datapath/hwx_$schemaname-$datapath-$tablename-$(date +\%Y-\%m-\%d).log 2>&1 &
                printf "\n===================================\n"
fi
