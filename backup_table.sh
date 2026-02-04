#!/bin/bash
ms_ip=192.168.16.101
ms_port_z=8880
rs_ip=192.168.16.101
rs_port=8500
ob_export_dir=/home/test/yaobase/bin/ob_export
db_name=$1
table_name=$2
mysql -h $ms_ip -P $ms_port_z -uadmin -padmin -e"using database $db_name;show create table $table_name;">backup_table.sql
sed -i '1d' backup_table.sql
$ob_export_dir -h $rs_ip -p $rs_port -t $db_name.$table_name -l $db_name.$table_name.log  -f $db_name.$table_name.dat --badfile $db_name.$table_name.bad --username admin --passwd admin --select-statement "select * from $db_name.$table_name;"
