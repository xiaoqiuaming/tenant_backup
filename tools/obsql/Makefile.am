AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/chunkserver/libyaodatasvr.a \
        $(top_builddir)/src/mergeserver/libyaosqlsvr.a \
        $(top_builddir)/src/rootserver/libyaoadminsvr.a \
        $(top_builddir)/src/updateserver/libyaotxnsvr.a \
	$(top_builddir)/src/common/compress/libcomp.a \
        $(top_builddir)/src/common/libcommon.a \
        $(top_builddir)/src/common/btree/libbtree.a \
        ${YYLIB_ROOT}/lib/libyynet.a \
        ${YYLIB_ROOT}/lib/libyysys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex ${GCOV_LIB} -lcrypto -lssl
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE -DCOMPATIBLE -D_BTREE_ENGINE_ #-O2 -fno-strict-aliasing

bin_PROGRAMS = yaosql
yaosql_SOURCES = obsql.cpp \
                client_rpc.cpp \
                base_client.cpp \
                ob_server_stats.cpp \
                stats.cpp \
                schema_printer.cpp \
                selectstmt.cpp \
                insertstmt.cpp \
                deletestmt.cpp \
                updatestmt.cpp \
		stmt.cpp \
                common_func.cpp


