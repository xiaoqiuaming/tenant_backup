AM_CPPFLAGS = \
				-I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash \
			  -I${top_srcdir}/src/common/compress

LDADD = \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a \
		$(top_builddir)/src/sstable/libsstable.a \
		libtestdiskpath.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		${EASY_LIB_PATH}/libeasy.a\
		${YYLIB_ROOT}/lib/libyynet.a\
		${YYLIB_ROOT}/lib/libyysys.a

AM_LDFLAGS = -lpthread -lc -g -lm -lrt -lgtest -ldl ${GCOV_LIB} -laio -lssl
CXXFLAGS += -D__GNU_SOURCE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_compact_sstable_writer

noinst_LIBRARIES = libtestdiskpath.a
libtestdiskpath_a_SOURCES = test_disk_path.cpp ob_fileinfo_cache.h ob_fileinfo_cache.cpp

test_compact_sstable_writer_SOURCES = test_compact_sstable_writer.cpp

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
