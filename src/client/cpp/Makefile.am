
LIBOCEANBASE_VERSION=2:0:1

lib_LTLIBRARIES = libyaoapi.la
libyaoapi_la_SOURCES = libobjoin.cpp libobapi.cpp ob_client.cpp obapi_main.c 
nodist_libyaoapi_la_SOURCES= $(top_srcdir)/git_version.c

libyaoapi_la_CPPFLAGS = -g -fPIC \
                       -I${YYLIB_ROOT}/include/yysys \
                       -I${YYLIB_ROOT}/include/yynet \
                       -I${top_srcdir}/src \
					   -DOBAPI_SO_VERSION=\"@obapi_version@\"

libyaoapi_la_LDFLAGS = -version-info $(LIBOCEANBASE_VERSION) \
                      -L${top_builddir}/src/common/ \
                      -L${YYLIB_ROOT}/lib/ \
                      -lcommon \
                      -Wl,-static,-lyynet \
                      -Wl,-static,-lyysys \
					  -Wl,-e,so_main \
                      -lpthread -lc -lm -lrt -ldl -laio

AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yynet \
			  -I${YYLIB_ROOT}/include/yysys \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/rootserver/libyaoadminsvr.a \
		${YYLIB_ROOT}/lib/libyynet.a \
		${YYLIB_ROOT}/lib/libyysys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex -lcrypto -lssl

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = set_sync_demo scan_sync_demo set_async_demo scan_async_demo \
							 sum_async_demo simple_test \
							 join_demo new_set_sync_demo new_scan_sync_demo
set_sync_demo_SOURCES = set_sync_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

scan_sync_demo_SOURCES = scan_sync_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

new_scan_sync_demo_SOURCES = new_scan_sync_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

new_set_sync_demo_SOURCES = new_set_sync_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

set_async_demo_SOURCES = set_async_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

scan_async_demo_SOURCES = scan_async_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

sum_async_demo_SOURCES = sum_async_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

#scan_all_SOURCES = scan_all.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

#test_expr_SOURCES = test_expr.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

#time_test_SOURCES = time_test.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

simple_test_SOURCES = simple_test.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

join_demo_SOURCES = join_demo.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

#test_stream_SOURCES = test_stream.c ${libyaoapi_la_SOURCES} ${nodist_libyaoapi_la_SOURCES}

EXTRA_DIST = \
	oceanbase.h \
	libobapi.h \
	libobjoin.h \
	ob_client.h

# rules to auto generate svn_version.cpp
include $(top_srcdir)/git_version.mk

