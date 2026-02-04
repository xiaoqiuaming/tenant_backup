NAME:     %NAME
Version:  %VERSION
Release:  %{RELEASE}
Summary:  TaoBao distributed database
Group:    Applications/Database
URL:      http://oceanbase.alibaba-inc.com/
Packager: taobao
License:  GPL
Vendor:   TaoBao
Prefix:   %{_prefix}
Source: %{NAME}-%{VERSION}.tar.gz
Buildroot: %(pwd)/%{name}-root
%if 0%{?el6}
BuildRequires: t-csrd-yynet-devel >= 1.0.9 lzo >= 2.06 snappy >=1.1.2 libaio-devel >= 0.3 t_libeasy-devel = 1.0.21-287.el6 openssl-devel >= 0.9.8 mysql-devel >= 5.1.0 taobao-jdk >= 1.6.0
%else
BuildRequires: t-csrd-yynet-devel >= 1.0.9 lzo >= 2.06 snappy >=1.1.2 libaio-devel >= 0.3 t_libeasy-devel = 1.0.21-287.el6 openssl-devel >= 0.9.8 mysql-devel >= 5.1.0 taobao-jdk >= 1.6.0
%endif
Requires: lzo = 20:2.0.6 snappy = 20:1.1.2 libaio >= 0.3 openssl >= 0.9.8
AutoReqProv: no

%package -n oceanbase-utils
summary: OceanBase utility programs
group: Development/Tools
Version: %VERSION
Release: %{RELEASE}

%package -n oceanbase-devel
summary: OceanBase client library
group: Development/Tools
Version: %VERSION
BuildRequires:curl >= 7.15.5 mysql-devel >= 5.1.0 t-db-congo-drcmessage = 0.1.3-47.el6 tb-lua-dev >= 5.1.4
Requires: curl >= 7.15.5 mysql-devel >= 5.1.0 t-db-congo-drcmessage = 0.1.3-47.el6 openssl-devel >= 0.9.8
Release: %{RELEASE}

%description
OceanBase is a distributed database

%description -n oceanbase-utils
OceanBase utility programs

%description -n oceanbase-devel
OceanBase client library

%define _unpackaged_files_terminate_build 0

%prep
%setup

%build

sh build.sh init
./configure RELEASEID=%{RELEASE} --prefix=%{_prefix} --with-test-case=no --with-release=yes --with-tblib-root=/opt/csr/common --with-easy-root=/usr --with-easy-lib-path=/usr/lib64 --with-drc-root=/home/ds
make %{?_smp_mflags}

%install
make DESTDIR=$RPM_BUILD_ROOT install
push $RPM_BUILD_DIR/%{NAME}-%{VERSION}/src/mrsstable
mvn assembly:assembly
cp target/mrsstable-1.0.1-SNAPSHOT-jar-with-dependencies.jar $RPM_BUILD_ROOT/%{_prefix}/bin/mrsstable.jar
popd

mkdir -p $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u
cp $RPM_BUILD_ROOT%{_prefix}/lib/liblzo_1.0.so     $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libmrsstable.so   $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libnone.so        $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libsnappy_1.0.so  $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0755, admin, admin)
%dir %{_prefix}/etc
%dir %{_prefix}/etc/supervison
%dir %{_prefix}/bin
%dir %{_prefix}/lib
%config(noreplace) %{_prefix}/etc/schema.ini
%config(noreplace) %{_prefix}/etc/lsyncserver.conf.template
%config %{_prefix}/etc/sysctl.conf
%config %{_prefix}/etc/snmpd.conf
%config %{_prefix}/etc/importserver.conf.template
%config %{_prefix}/etc/importcli.conf.template
%config %{_prefix}/etc/configuration.xml.template
%config %{_prefix}/etc/proxyserver.conf.template
%config %{_prefix}/etc/supervisor/lms.ini
%{_prefix}/bin/rootserver
%{_prefix}/bin/updateserver
%{_prefix}/bin/mergeserver
%{_prefix}/bin/chunkserver
%{_prefix}/bin/log_reader
%{_prefix}/bin/ob_import
%{_prefix}/bin/ob_export
%{_prefix}/bin/rs_admin
%{_prefix}/bin/ups_admin
%{_prefix}/bin/log_tool
%{_prefix}/bin/cs_admin
%{_prefix}/bin/ob_ping
%{_prefix}/bin/str2checkpoint
%{_prefix}/bin/checkpoint2str
%{_prefix}/bin/dumpsst
%{_prefix}/bin/backup_client
%{_prefix}/bin/backupserver
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
%{_prefix}/mrsstable_lib_5u/liblzo_1.0.so
%{_prefix}/mrsstable_lib_5u/libmrsstable.so
%{_prefix}/mrsstable_lib_5u/libnone.so
%{_prefix}/mrsstable_lib_5u/libsnappy_1.0.so
%{_prefix}/mrsstable_lib_5u/liblzo2.so
%{_prefix}/mrsstable_lib_5u/libsnappy


