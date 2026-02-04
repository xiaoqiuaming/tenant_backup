AM_CPPFLAGS = -I${top_srcdir}/src \
              -I${top_srcdir}/src/common \
              -I${top_srcdir}/tools/cbtop \
              -I${top_srcdir}/tools/cbtop/bson \
              -I${top_srcdir}/tools/cbtop/bson/lib \
              -I${top_srcdir}/tools/cbtop/bson/util \
              -I.

AM_CXXFLAGS = -g -O2 -D_OB_VERSION=400 -D_NO_EXCEPTION -finline-functions -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG -Wall -Wextra -Wunused-parameter -Wformat -fno-strict-aliasing -fno-omit-frame-pointer -Wconversion -Wno-deprecated -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -Wno-ignored-qualifiers
AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex ${GCOV_LIB} -laio -lcrypto -lssl -lcurses -lboost_program_options


confdir = ${prefix}/etc/conf

EXTRA_DIST= $(top_srcdir)/src/common/ob_define.h

bson_SOURCES = base64.cpp base64c.cpp bsonobj.cpp bson.hpp bsonc.cpp bsonDecimal.cpp \
               common_decimal.cpp encoding.cpp md5.cpp \
               nonce.cpp oid.cpp

noinst_PROGRAMS = bson
bson_LINK = true

