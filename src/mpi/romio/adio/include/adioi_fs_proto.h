/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/* 
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */


#ifndef ADIO_PROTO
#define ADIO_PROTO

#ifdef ROMIO_NFS
extern struct ADIOI_Fns_struct ADIO_NFS_operations;
/* prototypes are in adio/ad_nfs/ad_nfs.h */
#endif

#ifdef ROMIO_PANFS
extern struct ADIOI_Fns_struct ADIO_PANFS_operations;
/* prototypes are in adio/ad_panfs/ad_panfs.h */
#endif

#ifdef ROMIO_UFS
extern struct ADIOI_Fns_struct ADIO_UFS_operations;
/* prototypes are in adio/ad_ufs/ad_ufs.h */
#endif

#ifdef ROMIO_XFS
extern struct ADIOI_Fns_struct ADIO_XFS_operations;
/* prototypes are in adio/ad_xfs/ad_xfs.h */
#endif

#ifdef ROMIO_LUSTRE
extern struct ADIOI_Fns_struct ADIO_LUSTRE_operations;
/* prototypes are in adio/ad_lustre/ad_lustre.h */
#endif

#ifdef ROMIO_PVFS2
extern struct ADIOI_Fns_struct ADIO_PVFS2_operations;
/* prototypes are in adio/ad_pvfs2/ad_pvfs2.h */
#endif

#ifdef ROMIO_TESTFS
extern struct ADIOI_Fns_struct ADIO_TESTFS_operations;
/* prototypes are in adio/ad_testfs/ad_testfs.h */
#endif

#ifdef ROMIO_GPFS
extern struct ADIOI_Fns_struct ADIO_GPFS_operations;
/* prototypes are in adio/ad_gpfs/ad_gpfs.h */
#endif

#ifdef ROMIO_ICACHE
extern struct ADIOI_Fns_struct ADIO_ICACHE_operations;
/* prototypes are in adio/ad_icache/ad_icache.h */
#endif

#ifdef ROMIO_TRACE
extern struct ADIOI_Fns_struct ADIO_TRACE_operations;
/* prototypes are in adio/ad_trace/ad_trace.h */
#endif

#ifdef ROMIO_LOGFS
extern struct ADIOI_Fns_struct ADIO_LOGFS_operations;
#endif


#endif
