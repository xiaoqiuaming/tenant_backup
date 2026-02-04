EXTRA_DIST=build.sh rpm/oceanbase.spec git_dist_version rpm/oceanbase_test.spec rpm/oceanbase_test_R7.spec script/deploy script/dooba
sysconf_DATA=script/deploy/oceanbase.conf.template
bin_SCRIPTS=script/deploy/yaobase.pl script/dooba/dooba

if WITH_TEST_CASE
TEST_DIR:=tests
endif
SUBDIRS=src $(TEST_DIR) tools script
DIST_SUBDIRS=src tests tools script

utest: check
.PHONY: utest

# rules to auto generate git_version.cpp
include $(top_srcdir)/git_version.mk
