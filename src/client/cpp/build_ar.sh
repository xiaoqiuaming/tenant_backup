#!/bin/sh

TMP_DIR="__tmp$$"
PWD_DIR=`pwd`

mkdir -p $TMP_DIR

cp .libs/libyaoapi.a $TMP_DIR
cp ../../common/libcommon.a $TMP_DIR
cp ../../rootserver/libyaoadminsvr.a $TMP_DIR
cp $YYLIB_ROOT/lib/libyysys.a $TMP_DIR
cp $YYLIB_ROOT/lib/libyynet.a $TMP_DIR

cd $TMP_DIR

ar x libyaoapi.a
ar x libcommon.a
ar x libyaoadminsvr.a
ar x libyysys.a
ar x libyynet.a

ar cru $PWD_DIR/libyaoapi.a *.o
ranlib $PWD_DIR/libyaoapi.a

cd $PWD_DIR
rm -r $TMP_DIR

