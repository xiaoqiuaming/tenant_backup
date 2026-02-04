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

CS_DATA_PATH=CBASE_INSTALL_DIR/data/
#CS_DATA_PATH=/home/admin/oceanbase/data/
cs_datadir_list=`find ${CS_DATA_PATH} --regextype posix-basic -regex "^${CS_DATA_PATH}[0-9]\+$" | xargs`
for cs_datadir in ${cs_datadir_list[@]}
do
    cs_trash=${cs_datadir}/Recycle
    echo "delete cs Recycle data, dir=${cs_trash}"
    if [ -d ${cs_trash} ];then
        rm -f ${cs_trash}/*
    fi
done
