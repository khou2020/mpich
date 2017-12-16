/*****************************************************************
 *
 *   Helper functions for dealing with types and rtrees
 *
 *****************************************************************/

#ifndef LOGFS_RTREE_H
#define LOGFS_RTREE_H

#include "rtree.h"
#include "adio.h"


/* indicate invalid offset in tree */
#define ADIO_OFFSET_INVALID ((ADIO_Offset) -1)

typedef struct {
    rtree_handle rtree;
    ADIO_Offset rangesize;      /* sum of all ranges in tree */
} logfs_rtree;

/* add a continuous disk region to the rtree, splitting/removing existing
 * regions if needed */
void logfs_rtree_addsplit(logfs_rtree * tree,
                          ADIO_Offset start, ADIO_Offset stop, ADIO_Offset diskstart);


void logfs_rtree_type2tree(rtree_handle rtree, ADIO_Offset disp, MPI_Datatype
                           filetype, ADIO_Offset bytes);



typedef struct {
    int (*start) (void *data, int collective);
    int (*readstart) (void *buf, MPI_Datatype memtype, MPI_Datatype filetype, void *userdata);
    int (*readwait) (void *userdata);
    int (*writestart) (void *buf, MPI_Datatype type, int bytes, void *userdata);
    int (*writewait) (void *userdata);
    int (*stop) (void *data);
} logfs_rtree_flush_cb;

/* dump rtree to disk */
/* TODO: in collective mode, could make this smarter and try to have as
 * much write overlap when writing to the real file as possible
 * (or maybe just the opposite)
 *
 * If collective, bufsize has to be equal on all cpus */
void logfs_rtree_flush(logfs_rtree * tree, int bufsize,
                       const logfs_rtree_flush_cb * cb, void *cbdata, int coll,
                       ADIO_Offset * filesize, MPI_Comm comm);

#endif
