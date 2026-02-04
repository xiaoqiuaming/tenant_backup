#!/bin/sh

export AUTOM4TE="autom4te"
export AUTOCONF="autoconf"

case "x$1" in
xinit)
        set -x
        # printf 'release_1.3.2.5' > git_dist_branch
        # printf '33366699' > git_dist_version 
	cd src/sql
        rm -f sql_parser.lex.c sql_parser.lex.h sql_parser.tab.c sql_parser.tab.h sql_parser.output type_name.c
	sh gen_parser.sh
	cd ../..
        
	aclocal
	libtoolize --force --copy --automake
	autoconf --force
	automake --foreign --copy --add-missing -Woverride
	;;
xclean)
	echo 'cleaning...'
	make distclean >/dev/null 2>&1
	rm -rf autom4te.cache
	for fn in aclocal.m4 configure config.guess config.sub depcomp install-sh \
		ltmain.sh libtool missing mkinstalldirs config.log config.status Makefile; do
		rm -f $fn
	done

	find . -name Makefile.in -exec rm -f {} \;
	find . -name Makefile -exec rm -f {} \;
	find . -name .deps -prune -exec rm -rf {} \;
	echo 'done'
	;;
*)
	./configure
	;;
esac
