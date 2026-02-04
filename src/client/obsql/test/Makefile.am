cc=g++

AM_CPPFLAGS= -I${top_srcdir}/include\
             -I${YYLIB_ROOT}/include/yysys \
             -I${top_srcdir}/src \
             -I${top_srcdir}/src/client/obsql/src \
             -I${GTEST_ROOT}

AM_LDFLAGS=  -L/usr/lib64/mysql \
             -L${YYLIB_ROOT}/lib/ \
             -L${GTEST_ROOT}/lib \
             -L/usr/local/lib \
             -lpthread -lmysqlclient -lcurl -lboost_regex -lcrypto -lssl #-lgtest

LDADD = ${top_srcdir}/src/client/obsql/src/libyaosql.a \
        ${top_srcdir}/src/common/libcommon.a \
        ${YYLIB_ROOT}/lib/libyysys.a
bin_PROGRAMS=sqltest msqltest whiletest #dsutest gdstest
sqltest_SOURCES = ob_sql_test.cpp
msqltest_SOURCES = ob_sql_test_multh.cpp
whiletest_SOURCES = ob_sql_while_test.cpp
#dsutest_SOURCES = test_data_source_utility.cpp test_sql_base.cpp
#gdstest_SOURCES = test_group_data_source.cpp test_sql_base.cpp
