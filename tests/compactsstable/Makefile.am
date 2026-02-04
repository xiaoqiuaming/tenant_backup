AM_CPPFLAGS = -I${YYLIB_ROOT}/include/yysys \
			  -I${YYLIB_ROOT}/include/yynet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash

LDADD =  \
		 ${YYLIB_ROOT}/lib/libyynet.a \
		 ${top_builddir}/src/compactsstable/libcompactsstable.a \
		 ${top_builddir}/src/sstable/libsstable.a \
		 ${top_builddir}/src/chunkserver/libchunkserver.a \
		 $(top_builddir)/src/common/compress/libcomp.a \
		 ${top_builddir}/src/common/libcommon.a \
		 ${EASY_LIB_PATH}/libeasy.a \
		 ${YYLIB_ROOT}/lib/libyysys.a -lcrypt

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest   ${GCOV_LIB} -lcrypt
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_compact_row \
			   test_blockmem \
			   test_compactsstable_mem

test_compact_row_SOURCES = test_compact_row.cpp
test_blockmem_SOURCES = test_blockmem.cpp
test_compactsstable_mem = test_compactsstable_mem.cpp


check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
