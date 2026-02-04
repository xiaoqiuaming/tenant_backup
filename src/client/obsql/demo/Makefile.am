cc=g++
bin_PROGRAMS=sqldemo mtsqldemo trxdemo
sqldemo_SOURCES = ob_sql_demo.cpp
mtsqldemo_SOURCES = ob_sql_demo_mt.cpp
trxdemo_SOURCES = ob_sql_demo_transaction.cpp

AM_CPPFLAGS= -I${top_srcdir}/include\
             -I${YYLIB_ROOT}/include/yysys \
             -I${top_srcdir}/src

AM_LDFLAGS=  -L/usr/lib64/mysql \
             -L${YYLIB_ROOT}/lib/ \
             -lpthread -lyysys -lmysqlclient -lboost_regex -lcrypto -lssl

LDADD = ${YYLIB_ROOT}/lib/libyysys.a


