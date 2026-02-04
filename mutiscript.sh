#!/bin/sh
if [ $1 -eq 0 ];then
	ps -u admin|grep server
fi
if [ $1 -eq 1 ];then
	mysql -h 192.168.10.61 -P2880 -uadmin -padmin -c
fi
if [ $1 -eq 2 ];then
	cd yaobase_install
	make -j 10 -C src && make install
fi
if [ $1 -eq 3 ];then
	exec /home/admin/yaobase/commonscript/stop.sh
fi
if [ $1 -eq 4 ];then
	 exec /home/admin/yaobase/commonscript/start_eth0.sh
fi

if [ $1 -eq 5 ];then
	exec /home/admin/yaobase/commonscript/clear.sh
fi
if [ $1 -eq 6 ];then
	cd yaobase_install
	sh build.sh init
	./configure --prefix=/home/admin/yaobase --with-release=yes --with-test-case=no
	make -j 20 -C src
	make -j 20 -C tools
	make install
	cd tools/io_fault
	make
fi
if [ $1 -eq 7 ];then
	exec /home/admin/yaobase/delete.sh $2 $3 $4
fi
if [ $1 -eq 8 ];then
	exec /home/admin/yaobase/commonscript/clear.sh
	exec /home/admin/yaobase/commonscript/clear.sh
fi
if [ $1 -eq 9 ];then
         exec /home/admin/yaobase/commonscript/start_paxos.sh
fi
if [ $1 -eq 10 ];then
         exec /home/admin/yaobase/commonscript/start_multiups.sh
fi
