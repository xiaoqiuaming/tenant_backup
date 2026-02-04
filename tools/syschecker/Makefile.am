AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/chunkserver/libyaodatasvr.a \
		$(top_builddir)/src/mergeserver/libyaosqlsvr.a \
		$(top_builddir)/tools/syschecker/client/libclient.a \
		$(top_builddir)/src/rootserver/libyaoadminsvr.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${YYLIB_ROOT}/lib/libyynet.a \
		${YYLIB_ROOT}/lib/libyysys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lssl -laio -lboost_regex -lcrypto
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = syschecker

syschecker_SOURCES = main.cpp \
					 ob_syschecker_main.cpp \
					 ob_syschecker.cpp \
					 ob_syschecker_param.cpp \
					 ob_write_worker.cpp \
					 ob_read_worker.cpp \
					 ob_syschecker_stat.cpp \
					 ob_syschecker_rule.cpp \
					 ob_syschecker_schema.cpp

EXTRA_DIST = ob_syschecker_main.h \
			 ob_syschecker.h \
			 ob_syschecker_param.h \
			 ob_write_worker.h \
			 ob_read_worker.h \
			 ob_syschecker_stat.h \
			 ob_syschecker_rule.h \
			 ob_syschecker_schema.h

SUBDIRS=client
clean-local:
	-rm -f *.gcov *.gcno *.gcda
