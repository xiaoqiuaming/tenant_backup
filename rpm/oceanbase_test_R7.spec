#
# (C) 2007-2010 TaoBao Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# oceanbase_test.spec for hudson build test
# http://10.232.4.35:8765/hudson/job/oceanbase_zhuweng/
# Version: $id$
#
# Authors:
#   MaoQi maoqi@alipay.com
#

Name: %NAME
Version: %VERSION
Release: %{RELEASE}
Summary: YaoBase distributed database
Group: Applications/Databases
URL: http://oceanbase.alibaba-inc.com/
Packager: yaobase
License: GPL
Vendor: yaobase
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %(pwd)/%{name}-root
Requires: lzo >= 2.06 snappy >= 1.1.0 libaio >= 0.3 openssl >= 0.9.8 perl-DBI
AutoReqProv: no

%package -n yaobase-utils
summary: OceanBase utility programs
group: Development/Tools
Version: %VERSION
Release: %{RELEASE}

%package -n yaobase-devel
summary: OceanBase client library
group: Development/Libraries
Version: %VERSION
#BuildRequires: t-db-congo-drcmessage >= 0.1.2-40 tb-lua-dev >= 5.1.4
Requires: curl >= 7.15.5 mysql-devel >= 5.1.0
Release: %{RELEASE}

%description
OceanBase is a distributed database

%description -n yaobase-utils
OceanBase utility programs

%description -n yaobase-devel
OceanBase client library

%define _unpackaged_files_terminate_build 0

%prep
%setup

%build
chmod u+x build.sh
sh build.sh init
# use the following line for local bulid
./configure RELEASEID=%{RELEASE} --prefix=%{_prefix} --with-test-case=no --with-release=yes
make %{?_smp_mflags}

%install
make DESTDIR=$RPM_BUILD_ROOT install
#pushd $RPM_BUILD_DIR/%{NAME}-%{VERSION}/src/mrsstable
#mvn assembly:assembly
#cp target/mrsstable-1.0.1-SNAPSHOT-jar-with-dependencies.jar $RPM_BUILD_ROOT/%{_prefix}/bin/mrsstable.jar
#popd

mkdir -p $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u
mkdir -p $RPM_BUILD_ROOT%{_prefix}/scripts
cp $RPM_BUILD_ROOT%{_prefix}/lib/liblzo_1.0.so     $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libmrsstable.so   $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libnone.so        $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libsnappy_1.0.so  $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/

cp $RPM_BUILD_ROOT%{_prefix}/tools/free_cache.sh          $RPM_BUILD_ROOT%{_prefix}/scripts/
cp $RPM_BUILD_ROOT%{_prefix}/tools/log_cleaner.sh         $RPM_BUILD_ROOT%{_prefix}/scripts/
cp $RPM_BUILD_ROOT%{_prefix}/tools/rm_cs_trash_data.sh    $RPM_BUILD_ROOT%{_prefix}/scripts/
cp $RPM_BUILD_ROOT%{_prefix}/tools/rm_ups_trash_data.sh   $RPM_BUILD_ROOT%{_prefix}/scripts/

cp /usr/lib64/libboost_regex.so.1.53.0             $RPM_BUILD_ROOT%{_prefix}/lib/
cp /usr/lib64/libboost_program_options.so.1.53.0   $RPM_BUILD_ROOT%{_prefix}/lib/
cp /usr/lib64/libmysqlclient.so.18.0.0             $RPM_BUILD_ROOT%{_prefix}/lib/
cp /usr/lib64/libicui18n.so.50.1.2                 $RPM_BUILD_ROOT%{_prefix}/lib/
cp /usr/lib64/libicuuc.so.50.1.2                   $RPM_BUILD_ROOT%{_prefix}/lib/
cp /usr/lib64/libicudata.so.50.1.2                 $RPM_BUILD_ROOT%{_prefix}/lib/

%clean
rm -rf $RPM_BUILD_ROOT

%files
#%defattr(0755, admin, admin)
%dir %{_prefix}/etc
%dir %{_prefix}/bin
%dir %{_prefix}/lib
%dir %{_prefix}/tools
%dir %{_prefix}/scripts
%config(noreplace) %{_prefix}/etc/schema.ini
%config %{_prefix}/etc/lsyncserver.conf.template
%config %{_prefix}/etc/sysctl.conf
%config %{_prefix}/etc/snmpd.conf
%config %{_prefix}/etc/importserver.conf.template
%config %{_prefix}/etc/importcli.conf.template
%config %{_prefix}/etc/configuration.xml.template
%config %{_prefix}/etc/proxyserver.conf.template
%{_prefix}/bin/yaoadminsvr
%{_prefix}/bin/yaotxnsvr
%{_prefix}/bin/yaosqlsvr
%{_prefix}/bin/yaodatasvr
%{_prefix}/bin/proxyserver
%{_prefix}/bin/as_admin
%{_prefix}/bin/ts_admin
%{_prefix}/bin/log_tool
%{_prefix}/bin/ds_admin
%{_prefix}/bin/yao_ping
%{_prefix}/bin/str2checkpoint
%{_prefix}/bin/checkpoint2str
%{_prefix}/bin/lsyncserver
%{_prefix}/bin/dumpsst
%{_prefix}/bin/importserver.py
%{_prefix}/bin/importcli.py
%{_prefix}/bin/yao_import
%{_prefix}/bin/yao_export
%{_prefix}/bin/yao_check
%{_prefix}/bin/dumprt
%{_prefix}/bin/log_agent
%{_prefix}/bin/backup_client
%{_prefix}/bin/backupserver
#%{_prefix}/bin/mrsstable.jar
%{_prefix}/bin/yaobase.pl
%{_prefix}/lib/liblzo_1.0.a
%{_prefix}/lib/liblzo_1.0.la
%{_prefix}/lib/liblzo_1.0.so
%{_prefix}/lib/liblzo_1.0.so.0
%{_prefix}/lib/liblzo_1.0.so.0.0.0
%{_prefix}/lib/libmrsstable.a
%{_prefix}/lib/libmrsstable.la
%{_prefix}/lib/libmrsstable.so
%{_prefix}/lib/libmrsstable.so.0
%{_prefix}/lib/libmrsstable.so.0.0.0
%{_prefix}/lib/libnone.a
%{_prefix}/lib/libnone.la
%{_prefix}/lib/libnone.so
%{_prefix}/lib/libnone.so.0
%{_prefix}/lib/libnone.so.0.0.0
%{_prefix}/lib/libsnappy_1.0.a
%{_prefix}/lib/libsnappy_1.0.la
%{_prefix}/lib/libsnappy_1.0.so
%{_prefix}/lib/libsnappy_1.0.so.0
%{_prefix}/lib/libsnappy_1.0.so.0.0.0
%{_prefix}/lib/libboost_regex.so.1.53.0
%{_prefix}/mrsstable_lib_5u/liblzo_1.0.so
%{_prefix}/mrsstable_lib_5u/libmrsstable.so
%{_prefix}/mrsstable_lib_5u/libnone.so
%{_prefix}/mrsstable_lib_5u/libsnappy_1.0.so
%{_prefix}/mrsstable_lib_5u/liblzo2.so
%{_prefix}/mrsstable_lib_5u/libsnappy.so
%{_prefix}/mrsstable_lib_6u/liblzo_1.0.so
%{_prefix}/mrsstable_lib_6u/libmrsstable.so
%{_prefix}/mrsstable_lib_6u/libnone.so
%{_prefix}/mrsstable_lib_6u/libsnappy_1.0.so
%{_prefix}/mrsstable_lib_6u/liblzo2.so
%{_prefix}/mrsstable_lib_6u/libsnappy.so
%{_prefix}/tools/nodeops.py
%{_prefix}/scripts/free_cache.sh
%{_prefix}/scripts/log_cleaner.sh
%{_prefix}/scripts/rm_cs_trash_data.sh
%{_prefix}/scripts/rm_ups_trash_data.sh
%config %{_prefix}/etc/oceanbase.conf.template
#%{_prefix}/tests/

%files -n yaobase-utils
#%defattr(0755, admin, admin)
%{_prefix}/bin/yao_import
%{_prefix}/bin/yao_export
%{_prefix}/bin/as_admin
%{_prefix}/lib/libicui18n.so.50.1.2 
%{_prefix}/lib/libicudata.so.50.1.2 
%{_prefix}/lib/libicuuc.so.50.1.2 
%{_prefix}/lib/libmysqlclient.so.18.0.0
%{_prefix}/lib/libboost_program_options.so.1.53.0  
%{_prefix}/lib/libboost_regex.so.1.53.0 

%files -n yaobase-devel
#%defattr(0755, admin, admin)
#%{_prefix}/lib/liboblog.so.1.0.0
#%{_prefix}/lib/liboblog.a
#%config(noreplace) %{_prefix}/etc/liboblog.conf
#%config(noreplace) %{_prefix}/etc/liboblog.partition.lua
%dir %{_prefix}/include
%{_prefix}/include/ob_define.h
#%{_prefix}/include/liboblog.h

%post
#chown -R admin:admin $RPM_INSTALL_PREFIX
sed -i "s!CBASE_INSTALL_DIR!$RPM_INSTALL_PREFIX!g" $RPM_INSTALL_PREFIX/scripts/log_cleaner.sh
sed -i "s!CBASE_INSTALL_DIR!$RPM_INSTALL_PREFIX!g" $RPM_INSTALL_PREFIX/scripts/rm_cs_trash_data.sh
sed -i "s!CBASE_INSTALL_DIR!$RPM_INSTALL_PREFIX!g" $RPM_INSTALL_PREFIX/scripts/rm_ups_trash_data.sh

%post -n yaobase-utils
#chown -R admin:admin $RPM_INSTALL_PREFIX
