## -*- Mode: Makefile; -*-
## vim: set ft=automake :
##
## (C) 2011 by Argonne National Laboratory.
##     See COPYRIGHT in top-level directory.
##

if BUILD_AD_LOGFS

AM_CPPFLAGS += -I$(top_srcdir)/adio/ad_logfs -I$(top_srcdir)/adio/ad_icache/cachelib

noinst_HEADERS += adio/ad_logfs/ad_logfs_common.h\
		  adio/ad_logfs/ad_logfs.h\
		  adio/ad_logfs/logfs_file.h\
		  adio/ad_logfs/logfs.h\
		  adio/ad_logfs/logfs_info.h\
		  adio/ad_logfs/logfs_rtree.h\
		  adio/ad_logfs/logfs_user.h\
		  adio/ad_logfs/rtree_config.h \
                  adio/ad_icache/cachelib/rtree.h

romio_other_sources += adio/ad_logfs/ad_logfs.c \
		       adio/ad_logfs/ad_logfs_close.c \
		       adio/ad_logfs/ad_logfs_common.c\
		       adio/ad_logfs/ad_logfs_delete.c \
		       adio/ad_logfs/ad_logfs_done.c\
		       adio/ad_logfs/ad_logfs_fcntl.c\
		       adio/ad_logfs/ad_logfs_flush.c\
		       adio/ad_logfs/ad_logfs_getsh.c\
		       adio/ad_logfs/ad_logfs_hints.c\
		       adio/ad_logfs/ad_logfs_iread.c\
		       adio/ad_logfs/ad_logfs_iwrite.c\
		       adio/ad_logfs/ad_logfs_open.c\
		       adio/ad_logfs/ad_logfs_rdcoll.c \
		       adio/ad_logfs/ad_logfs_read.c\
		       adio/ad_logfs/ad_logfs_resize.c\
		       adio/ad_logfs/ad_logfs_seek.c\
		       adio/ad_logfs/ad_logfs_setsh.c\
		       adio/ad_logfs/ad_logfs_wait.c\
		       adio/ad_logfs/ad_logfs_wrcoll.c\
		       adio/ad_logfs/ad_logfs_write.c\
		       adio/ad_logfs/logfs.c\
		       adio/ad_logfs/logfs_file.c\
		       adio/ad_logfs/logfs_rtree.c\
		       adio/ad_logfs/ad_logfs_features.c
endif BUILD_AD_LOGFS