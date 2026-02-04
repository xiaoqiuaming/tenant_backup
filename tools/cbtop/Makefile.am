SUBDIRS = bson

AM_CPPFLAGS = -g -fPIC -I${YYLIB_ROOT}/include/yysys \
	      -I${TBLIB_ROOT}/include/yynet \
	      -I${EASY_ROOT}/include/easy \
	      -I${top_srcdir}/include \
	      -I${top_srcdir}/src \
	      -I${top_srcdir}/src/common \
	      -I${top_srcdir}/tools/cbtop \
	      -I${top_srcdir}/tools/cbtop/bson \
	      -I${top_srcdir}/tools/cbtop/bson/lib \
	      -I${top_srcdir}/tools/cbtop/bson/util \
	      -I.
INCLUDES = -L${top_srcdir}/tools/cbtop/bson:${top_srcdir}/tools/cbtop

LDADD = $(top_builddir)/src/updateserver/libyaotxnsvr.a \
	$(top_builddir)/src/rootserver/libyaoadminsvr.a \
	$(top_builddir)/src/chunkserver/libyaodatasvr.a \
	$(top_builddir)/src/mergeserver/libyaosqlsvr.a \
	$(top_builddir)/src/sql/libsql.a \
	$(top_builddir)/src/sstable/libsstable.a \
	$(top_builddir)/src/common/compress/libcomp.a \
	$(top_builddir)/src/common/libcommon.a \
	$(top_builddir)/src/common/timezone/libtimezone.a \
	$(top_builddir)/src/common/btree/libbtree.a \
	${YYLIB_ROOT}/lib/libyynet.a \
	${EASY_LIB_PATH}/libeasy.a \
	${YYLIB_ROOT}/lib/libyysys.a -lcrypt \
	${top_srcdir}/tools/cbtop/bson/base64c.o \
	${top_srcdir}/tools/cbtop/bson/base64.o \
	${top_srcdir}/tools/cbtop/bson/bsonc.o \
	${top_srcdir}/tools/cbtop/bson/bsonDecimal.o \
	${top_srcdir}/tools/cbtop/bson/bsonobj.o \
	${top_srcdir}/tools/cbtop/bson/common_decimal.o \
	${top_srcdir}/tools/cbtop/bson/encoding.o \
	${top_srcdir}/tools/cbtop/bson/md5.o \
	${top_srcdir}/tools/cbtop/bson/nonce.o \
	${top_srcdir}/tools/cbtop/bson/oid.o

AM_CXXFLAGS = -g -O2 -D_OB_VERSION=400 -D_NO_EXCEPTION -finline-functions -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG -Wall -Wextra -Wunused-parameter -Wformat -fno-strict-aliasing -fno-omit-frame-pointer -Wconversion -Wno-deprecated -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -Wno-ignored-qualifiers
AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex ${GCOV_LIB} -laio -lcrypto -lcrypt -lssl -lcurses -lboost_program_options -ltinfo

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

conf_DATA = ./conf/samples/ybtop.xml
confdir = ${prefix}/etc/conf

EXTRA_DIST= cbtop.h core.h oss.h ossUtil.h ossMem.h \
	    bson/util/builder.h bson/util/optime.h bson/util/embedded_builder.h \
	    bson/util/hex.h bson/util/misc.h bson/util/time_support.h \
	    bson/bson_db.h bson/bson-inl.h bson/bsontypes.h bson/bsonmisc.h \
	    bson/bsonc.h bson/bsonnoncopyable.h bson/bson.h bson/lib/md5.h bson/lib/hex.h \
	    bson/lib/nonce.h bson/lib/atomic_int.h bson/lib/base64.h bson/lib/md5.hpp \
	    bson/common_decimal_type.h bson/base64c.h bson/encoding.h bson/bsonintrusiveptr.h \
	    bson/ordering.h bson/common_decimal_fun.h bson/oid.h bson/bsonobjiterator.h \
	    bson/bsonelement.h bson/bsonassert.h bson/stringdata.h bson/bsonobj.h \
	    bson/bsonobjbuilder.h bson/inline_decls.h bson/bsonDecimal.h \
	    ncurses/wcwidth.h ncurses/tty/tty_input.h ncurses/tty/tty_display.h \
	    ncurses/init_keytry.h ncurses/fifo_defs.h \
	    ncurses/build.priv.h ncurses/SigAction.h ncurses/curses.priv.h \
	    ncurses/include/wcwidth.h ncurses/include/panel.h ncurses/include/eti.h \
	    ncurses/include/ncurses_cfg.h ncurses/include/init_keytry.h \
	    ncurses/include/nc_panel.h ncurses/include/ncurses_def.h ncurses/include/nc_mingw.h \
	    ncurses/include/unctrl.h ncurses/include/fifo_defs.h ncurses/include/nc_alloc.h \
	    ncurses/include/config.h ncurses/include/ncurses_mingw.h \
	    ncurses/include/build.priv.h ncurses/include/hashsize.h ncurses/include/SigAction.h \
	    ncurses/include/term_entry.h ncurses/include/mf_common.h \
	    ncurses/include/curses.priv.h ncurses/include/menu.h ncurses/include/curses.h \
	    ncurses/include/term.h ncurses/include/form.h ncurses/include/termcap.h \
	    ncurses/include/hashed_db.h ncurses/include/tic.h ncurses/include/ncurses_dll.h \
	    ncurses/include/parametrized.h ncurses/include/nc_tparm.h \
	    $(top_srcdir)/tools/cbtop/conf/samples/ybtop.xml

cbtop_SOURCES = cbtop.cpp oss.hpp ossMem.cpp ossUtil.cpp ossUtil.hpp

bin_PROGRAMS = cbtop

clean-local:
	-rm -f *.gcov *.gcno *.gcda

