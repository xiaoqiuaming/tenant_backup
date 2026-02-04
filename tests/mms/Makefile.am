AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/clustermanager \
			  -I${top_srcdir}/src/mms

LDADD = ${top_builddir}/src/mms/libmms.a \
        ${top_builddir}/src/clustermanager/libocm.a \
        ${top_builddir}/src/common/libcommon.a \
		${YYLIB_ROOT}/lib/libyynet.a \
		${YYLIB_ROOT}/lib/libyysys.a  

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest   ${GCOV_LIB}
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif
bin_PROGRAMS = test_mms 

test_mms_SOURCES = test_mms.cpp mock_node_server.cpp mock_server.cpp mock_monitor_server.cpp mock_monitor_server.h  mock_node_server.h  mock_server.h

