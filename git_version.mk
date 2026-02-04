# rules to auto generate git_version.cpp
BUILD_SOURCE=$(top_srcdir)/git_version.cpp $(top_srcdir)/git_dist_version $(top_srcdir)/git_dist_branch
CLEANFILES=$(top_srcdir)/git_version.cpp $(top_srcdir)/git_version.c $(top_srcdir)/git_dist_version $(top_srcdir)/git_dist_branch
$(top_srcdir)/git_version.cpp: FORCE
	echo -n 'const char* git_version() {const char* GIT_Version = "' > $@
#	@mygitbranch@ | cat | tr "\n" "_" >> $(top_srcdir)/git_version.cpp
	@mygitbranch@ | cat | tr "\n" "0" >> $(top_srcdir)/git_version.cpp
#	@mygitclean@ | cat | sed  's/1/Clean/g' | sed 's/0/Dirty/g' | tr "\n" "_" >> $(top_srcdir)/git_version.cpp
	@mygitcommitid@ | cat | tr "\n" " " >> $(top_srcdir)/git_version.cpp
	echo '"; return GIT_Version; }' >> $@
	echo 'const char* build_date() { return __DATE__; }' >> $@
	echo 'const char* build_time() { return __TIME__; }' >> $@
	echo -n 'const char* build_flags() { const char* build_flag="' >> $@
	echo -n $(CXXFLAGS) $(CPPFLAGS) | sed s/\"//g >> $@
	echo -n ' --tag ' | tr "\n" " " >> $@
	@mygittag@ | cat | tr "\n" " " >> $(top_srcdir)/git_version.cpp
	echo '";  return build_flag; }' >> $@
	cp $@ $(top_srcdir)/git_version.c

#if HAVESVNWC
$(top_srcdir)/git_dist_version: FORCE
	@mygitcommitid@ > $@
#endif

$(top_srcdir)/git_dist_branch: FORCE
	@mygitbranch@ > $@
	
FORCE:
