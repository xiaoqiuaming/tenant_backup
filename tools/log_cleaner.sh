#!/bin/bash

#location logs lies
LOGPATH=${1:-"CBASE_INSTALL_DIR/log/"}

#days to expire, log files older than ${EXPIRE} will be removed
EXPIRE=${2:-15}

TMPFILE="./old_log_files"

echo "log=$LOGPATH, expire=${EXPIRE}"

find ${LOGPATH} -regextype posix-basic -regex "${LOGPATH}[a-z]\+.log\+.[wf]*\+.[0-9]\+" -a -mtime "+${EXPIRE}" > ${TMPFILE}
if [ $? -ne 0 ];then
  echo "find older log files failed"
  exit 1
fi

for f in `cat ${TMPFILE}`
do
  /usr/sbin/lsof $f > /dev/null 2>&1
  if [ $? -eq 0 ];then
    echo "$f is still open"
  else
    echo "deleting file:$f"
    rm -f $f
  fi
done
