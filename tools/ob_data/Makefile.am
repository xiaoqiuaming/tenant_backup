AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
							-I${YYLIB_ROOT}/include/yynet \
							-I$(top_builddir)/src/common/ \
							-I$(top_builddir)/src/mergeserver \
							-I${top_srcdir}/src

LDADD = $(top_builddir)/src/mergeserver/libyaosqlsvr.a \
				$(top_builddir)/src/common/libcommon.a \
				${YYLIB_ROOT}/lib/libyynet.a \
				${YYLIB_ROOT}/lib/libyysys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex -lcrypto -lssl
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = yao_data mock_yaosql_svr
yao_data_SOURCES =   main.cpp \
										ob_client.cpp \
                    rpc_stub.cpp \
									  base_client.cpp
mock_yaosql_svr_SOURCES = test_merge_server_main.cpp \
														mock_merge_server.cpp \
														mock_server.cpp

EXTRA_DIST =  base_client.h \
              ob_client.h \
							rpc_stub.h \
							mock_merge_server.h \
							mock_server.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda
