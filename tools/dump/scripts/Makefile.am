LDADD = $(top_builddir)/src/updateserver/libyaotxnsvr.a \
				$(top_builddir)/src/sstable/libsstable.a \
				$(top_builddir)/src/chunkserver/libyaodatasvr.a \
				$(top_builddir)/src/rootserver/libyaoadminsvr.a \
				$(top_builddir)/src/mergeserver/libyaosqlsvr.a \
				$(top_builddir)/src/common/compress/libcomp.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${YYLIB_ROOT}/lib/libyynet.a \
				${YYLIB_ROOT}/lib/libyysys.a
EXTRA_DIST= dumper.ini dumper.ini_master obdump.sh ob_check_done.sh
pkgdata_data_DATA= dumper.ini dumper.ini_master obdump.sh ob_check_done.sh
pkgdata_datadir=${prefix}/etc
