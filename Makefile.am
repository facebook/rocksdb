# $Id: Makefile.am,v 1.0.0-0 2016/05/06 14:24:14 Cnangel Exp $

AUTOMAKE_OPTIONS	= foreign 

EXTRA_DIST			= autogen.sh \
					  $(wildcard m4/*.m4) \
					  $(wildcard pkgconfig/*.pc) \
					  $(wildcard mypkgconfig/*.pc) \
					  $(NULL)

ACLOCAL_AMFLAGS		= -I m4

SUBDIRS				= port \
					  util \
					  utilities \
					  table \
					  db \
					  memtable \
					  . \
					  test \
					  tools \
					  examples \
					  $(NULL)


# install pc files
pcdatadir			= $(libdir)/pkgconfig
pcdata_DATA			= pkgconfig/@PACKAGE@.pc \
					  $(NULL)

# install @PACKAGE@-config
bin_SCRIPTS			= scripts/@PACKAGE@-config


lib_LTLIBRARIES						= librocksdb.la
librocksdb_la_SOURCES				= $(NULL)
librocksdb_la_LIBADD				= $(top_builddir)/port/librocksdb_port.la \
									  $(top_builddir)/util/librocksdb_util.la \
									  $(top_builddir)/table/librocksdb_tbl.la \
									  $(top_builddir)/db/librocksdb_db.la \
									  $(top_builddir)/memtable/librocksdb_memtbl.la \
									  $(top_builddir)/utilities/table_properties_collectors/librocksdb_tpc.la \
									  $(top_builddir)/utilities/redis/librocksdb_redis.la \
									  $(top_builddir)/utilities/backupable/librocksdb_bakable.la \
									  $(top_builddir)/utilities/ttl/librocksdb_ttl.la \
									  $(top_builddir)/utilities/compaction_filters/librocksdb_cf.la \
									  $(top_builddir)/utilities/transactions/librocksdb_trans.la \
									  $(top_builddir)/utilities/document/librocksdb_document.la \
									  $(top_builddir)/utilities/spatialdb/librocksdb_spatialdb.la \
									  $(top_builddir)/utilities/write_batch_with_index/librocksdb_wbwi.la \
									  $(top_builddir)/utilities/flashcache/librocksdb_flashcache.la \
									  $(top_builddir)/utilities/memory/librocksdb_memory.la \
									  $(top_builddir)/utilities/librocksdb_utilities.la \
									  $(top_builddir)/utilities/convenience/librocksdb_convenience.la \
									  $(top_builddir)/utilities/leveldb_options/librocksdb_leveldb_options.la \
									  $(top_builddir)/utilities/merge_operators/string_append/librocksdb_string_append.la \
									  $(top_builddir)/utilities/merge_operators/librocksdb_merge_operators.la \
									  $(top_builddir)/utilities/geodb/librocksdb_geodb.la \
									  $(top_builddir)/utilities/options/librocksdb_options.la \
									  $(top_builddir)/utilities/checkpoint/librocksdb_chkpoint.la \
									  $(NULL)
# librocksdb_la_LDFLAGS				= -no-undefined -version-info @ROCKSDB_MAJOR@:@ROCKSDB_MINOR@:@ROCKSDB_PATCH@

# define rpm build root
RPMBUILDTOPDIR		= $(shell unset RPMBUILDTOPDIR; if [ -f ${HOME}/.rpmmacros ]; then export RPMBUILDTOPDIR=`grep _topdir ${HOME}/.rpmmacros|head -1|awk '{print $$2}'`; else if [ -z $(RDIR) ]; then export RPMBUILDTOPDIR=target; else export RPMBUILDTOPDIR=$(RDIR); fi; fi; echo $${RPMBUILDTOPDIR};)
PROCESSOR = $(shell uname -p)


.PHONY: rpm doc

doc:
	$(mkdir_p) @DOXYGEN_OUTPUT_DIR@
	doxygen -u && doxygen
	sed -i -e "s/charset=iso-8859-1/charset=utf-8/g" $(top_builddir)/@DOXYGEN_OUTPUT_DIR@/html/*.html

rpm: dist @PACKAGE@.spec
	$(mkdir_p) ${RPMBUILDTOPDIR}/{BUILD,RPMS,SOURCES,SPECS,SRPMS,BUILDROOT}
	$(RM) ${RPMBUILDTOPDIR}/SOURCES/$(distdir).tar.gz
	$(RM) ${RPMBUILDTOPDIR}/SPECS/@PACKAGE@.spec
	$(LN_S) `pwd`/$(distdir).tar.gz ${RPMBUILDTOPDIR}/SOURCES
	$(LN_S) `pwd`/@PACKAGE@.spec ${RPMBUILDTOPDIR}/SPECS
	rpmbuild -ba ${RPMBUILDTOPDIR}/SPECS/@PACKAGE@.spec $(OPTIONS)

CLEANDIRS				= doc/doxygen
DISTCLEANFILES			= .deps Makefile 
CLEANFILES				= *~ *.log @PACKAGE@-*.tar.gz @PACKAGE@-config
MAINTAINERCLEANFILES	= Makefile.in

clean-local:
	-test -z "$(CLEANDIRS)" || rm -rf $(CLEANDIRS)

