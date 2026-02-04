#!/bin/bash

self=$0
cnt=`/bin/ps -ef | /bin/grep $self | /bin/grep -v grep | /usr/bin/wc -l`
echo $self

/bin/ps -ef | /bin/grep $self | /bin/grep -v grep
echo $cnt

if [ $cnt -gt 4 ]
then
    echo "WARN:parallel process > 3"
    #exit 0
fi

UPS_DATA_PATH=CBASE_INSTALL_DIR/data/ups_data
#UPS_DATA_PATH=/home/admin/oceanbase/data/ups_data
UPS_TRASH_PATH=${UPS_DATA_PATH}/raid*/store*/trash

ls -l ${UPS_TRASH_PATH}
rm -f ${UPS_TRASH_PATH}/*
