AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src\
			  -I${top_srcdir}/src/common\
			  -I${top_srcdir}/src/updateserver\
			  -I${top_srcdir}/src/clustermanager

CXXFLAGS += -D__UNIT_TEST__
LDADD = \
    $(top_builddir)/src/clustermanager/libyaoadminsvr.a \
    $(top_builddir)/src/updateserver/libyaotxnsvr.a \
    ${top_builddir}/src/clustermanager/libyaoadminsvr.a \
    ${top_builddir}/src/common/compress/libcomp.a \
    ${top_builddir}/src/common/libcommon.a \
		${top_builddir}/src/common/btree/libbtree.a \
		${YYLIB_ROOT}/lib/libyynet.a \
		${YYLIB_ROOT}/lib/libyysys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex -lcrypto -lssl
CXXFLAGS+=-D__VERSION_ID__="\"updateserver 1.0.0.0\""
CXXFLAGS+=-D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif
noinst_LIBRARIES = libmms.a
libmms_a_SOURCES = \
  ob_monitor.cpp \
	ob_node.cpp\
  ob_mms_checklease_task.cpp\
  ob_mms_heartbeat_task.cpp

EXTRA_DIST = \
  ob_monitor.h \
	ob_node.h\
  ob_mms_checklease_task.h\
  ob_mms_heartbeat_task.h
