AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/sql \
			  -I${top_srcdir}/src/rootserver
LIBTOOLFLAGS=--preserve-dup-deps
LDADD = $(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
		$(top_builddir)/src/common/libcommon.a \
		${EASY_LIB_PATH}/libeasy.a \
		${YYLIB_ROOT}/lib/libyysys.a

AM_LDFLAGS = -lpthread -lc -lm  -lgtest -lgmock -lrt ${GCOV_LIB} -lssl -laio
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS =  base_main_test \
			  test_root_monitor_table \
				yao_obi_role_test\
        test_batch_create_table\
				yaodata_svrs_manager_test \
				test_root_table2 \
				yao_ts_manager_test \
				test_tablet_info_manager\
       test_root_table2_test\
				yaoadmin_svr_test\
       yao_new_balance_test\
       yao_delete_replicas_test

test_root_monitor_table_SOURCES = test_root_monitor_table.cpp
nodist_test_root_monitor_table_SOURCES = $(top_srcdir)/git_version.cpp    
yaodata_svrs_manager_test_SOURCES = chunk_servers_manager_test.cpp
base_main_test_SOURCES = base_main_test.cpp test_main.cpp
yaoadmin_svr_test_SOURCES = root_server_test.cpp
nodist_yaoadmin_svr_test_SOURCES = $(top_srcdir)/git_version.cpp    
#yaoadmin_svr_log_test_SOURCES = root_server_log_test.cpp
test_tablet_info_manager_SOURCES = test_tablet_info_manager.cpp
test_root_table2_SOURCES = test_root_table2.cpp $(top_srcdir)/git_version.cpp
test_root_table2_test_SOURCES = test_root_table2_test.cpp $(top_srcdir)/git_version.cpp
yao_obi_role_test_SOURCES = ob_obi_role_test.cpp
yao_ts_manager_test_SOURCES = ob_ups_manager_test.cpp
nodist_yao_ts_manager_test_SOURCES = $(top_srcdir)/git_version.cpp
yao_new_balance_test_SOURCES = ob_new_balance_test.cpp
nodist_yao_new_balance_test_SOURCES = $(top_srcdir)/git_version.cpp    
yao_delete_replicas_test_SOURCES = ob_delete_replicas_test.cpp
nodist_yao_delete_replicas_test_SOURCES = $(top_srcdir)/git_version.cpp    
test_batch_create_table_SOURCES = test_batch_create_table.cpp
#EXTRA_DIST = mock_chunk_server.h  mock_server.h  mock_update_server.h  root_server_tester.h  test_main.h mock_root_rpc_stub.h
EXTRA_DIST = test_main.h \
						 mock_root_rpc_stub.h \
						 mock_chunk_server.h  \
						 mock_server.h \
						 mock_update_server.h  \
						 root_server_tester.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
