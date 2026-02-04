AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/chunkserver/libyaodatasvr.a \
	$(top_builddir)/src/mergeserver/libyaosqlsvr.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${YYLIB_ROOT}/lib/libyynet.a \
		${YYLIB_ROOT}/lib/libyysys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex -lcrypto -lssl

noinst_LIBRARIES = libclient.a
libclient_a_SOURCES = ob_client.cpp \
					  ob_server_manager.cpp \
					  ob_server_rpc.cpp

EXTRA_DIST = ob_client.h \
			 ob_server_manager.h \
			 ob_server_rpc.h
