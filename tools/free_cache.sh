#!/bin/bash

total=`free -m | awk 'NR==2' | awk '{print $2}'`
used=`free -m | awk 'NR==2' | awk '{print $3}'`
free=`free -m | awk 'NR==2' | awk '{print $4}'`
cached=`free -m | awk 'NR==2' | awk '{print $7}'`
threshold=`echo "${total}/10" | bc`

if [ ${cached} -gt ${threshold} ]
then
    echo "====================" >> /var/log/free_cache.log
    date >> /var/log/free_cache.log
    echo "Before:   Memory usage | Used:${used}MB | Free:${free}MB | Cached:${cached}MB" >> /var/log/free_cache.log

    sync && echo 1 > /proc/sys/vm/drop_caches
    sleep 3s

    used=`free -m | awk 'NR==2' | awk '{print $3}'`
    free=`free -m | awk 'NR==2' | awk '{print $4}'`
    cached=`free -m | awk 'NR==2' | awk '{print $7}'`
    echo "After:    Memory usage | Used:${used}MB | Free:${free}MB | Cached:${cached}MB" >> /var/log/free_cache.log
    echo "free OK" >>  /var/log/free_cache.log
else
    echo "====================" >> /var/log/free_cache.log
    date >> /var/log/free_cache.log
    echo "Status is OK:   Memory usage | Used:${used}MB | Free:${free}MB | Cached:${cached}MB" >> /var/log/free_cache.log
fi