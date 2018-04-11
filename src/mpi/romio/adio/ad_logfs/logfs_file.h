/****************************************************************************
 *  Allows reading/writing of a logfs trace log                             *
 *
 *    Only deals with the logfs logfiles;
 *      => recording and rereading journal entries                          *
 *                                                                          *
 ****************************************************************************/

#ifndef LOGFS_FILE_H
#define LOGFS_FILE_H

#include "adio.h"

#define LOGFS_FILE_LOG_DATA 1
#define LOGFS_FILE_LOG_META 2

/* for debugging */
#define LOGFS_FILE_DEBUG

/* ======================================================================== */
/* == Callbacks =========================================================== */
/* ======================================================================== */

typedef struct
{
    /* init should prepare both files (meta + data) for writing;
     * Needs to set metaofs and dataofs to filesizes */
    int (*init)(void *userdata);

    /* log will be one of LOGFS_FILE_LOG_DATA or LOGFS_FILE_LOG_META */
    int (*write)(void *userdata, ADIO_Offset ofs, const void *data, int size, int log);

    int (*done)(void *userdata);

    /* this function should truncate the specified logfile to the given offset */
    int (*restart)(void *userdata, ADIO_Offset offset, int log);

    /* return filesize; will be called before data is written when reopening
     * logfiles */
    int (*getsize)(void *userdata, ADIO_Offset *offset, int log);
} logfs_file_ops;

typedef struct
{
    /* function should open metadatalog for reading */
    int (*init)(void *userdata);

    /* read from file; return number of bytes read */
    int (*read)(void *userdata, ADIO_Offset offset, void *data, int size, int log);

    /* close metadata log */
    int (*done)(void *userdata);
} logfs_file_readops;

/* ======================================================================== */
/* === Datatype =========================================================== */
/* ======================================================================== */

/* flatlist like structure */
typedef struct
{
    MPI_Count count;
    ADIO_Offset *blocklens;
    ADIO_Offset *indices;
} logfs_file_typeinfo;

typedef struct
{
    int (*init)(void *data);
    int (*start_epoch)(void *data, int epoch);

    /* user gets ownership of the ftype and datarep pointers and needs to free
     * them [datarep is constant for now ]*/
    int (*set_view)(void *data, ADIO_Offset displacement,
                    logfs_file_typeinfo *etype, logfs_file_typeinfo *ftype, const char *datarep);
    int (*set_size)(void *data, ADIO_Offset size);
    int (*write)(void *data, ADIO_Offset fileofs, int size, ADIO_Offset datalogofs);
    int (*done)(void *data);
} logfs_file_replayops;

/* ======================================================================== */
/* == Instance type ======================================================= */
/* ======================================================================== */

struct logfs_file_instance;

typedef struct logfs_file_instance *logfs_file_handle;
typedef const struct logfs_file_instance *logfs_file_consthandle;

/* ======================================================================== */
/* === Public Functions =================================================== */
/* ======================================================================== */

logfs_file_handle logfs_file_create(MPI_Comm comm,
                                    const logfs_file_ops *ops, void *ops_data,
                                    const logfs_file_readops *readops, void *readops_data);

/* free logfs file instance */
void logfs_file_free(logfs_file_handle *handle);

/* access functions */
/* collective! */
void logfs_file_record_sync(logfs_file_handle handle);

void logfs_file_record_view(logfs_file_handle, MPI_Datatype etype,
                            MPI_Datatype filetype, MPI_Offset displacement, const char *datarep);

/* offset should be the high-level MPI offset (meaning expressed in etypes and
 * taking displacement of the current view into account)
 * Returns start of datablock in data logfile */
MPI_Offset logfs_file_record_write(logfs_file_handle, const void *buf, int count,
                                   MPI_Datatype memtype, MPI_Offset offset);

void logfs_file_record_setsize(logfs_file_handle handle, ADIO_Offset size);

/* flush log files */
void logfs_file_flush(logfs_file_handle handle);

/* replay logfile: last=1: only last epoch, last=0 everything  */
/* returns 1 if all OK, 0 if one of the callbacks returned 0 */
int logfs_file_replay(logfs_file_handle handle, int last,
                      const logfs_file_replayops *ops, void *opsdata);

/* clear (truncate) logfiles; last=1: only last epoch; last=0 everything */
void logfs_file_clear(logfs_file_handle handle, int last);

/* set epoch number; collective and must be same on all cpus */
void logfs_file_setepoch(logfs_file_handle handle, int epoch);

/* ======================================================================== */
/* ======================================================================== */
/* ======================================================================== */

/* helper function for typeinfo structures */
static inline int logfs_file_typeinfo_extent(const logfs_file_typeinfo *info)
{
    const int last = info->count - 1;
    int extent = info->indices[last] + info->blocklens[last] - info->indices[0];
    return extent;
}

static inline int logfs_file_typeinfo_size(const logfs_file_typeinfo *info)
{
    int ret = 0;
    int i;
    for (i = 0; i < info->count; ++i)
        ret += info->blocklens[i];
    return ret;
}

static inline int logfs_file_typeinfo_continuous(const logfs_file_typeinfo *info)
{
    int i;
    int last = info->indices[0] + info->blocklens[0];
    for (i = 1; i < info->count; ++i)
    {
        if (info->indices[i] != last)
            return 0;
        last += info->blocklens[i];
    }
    return 1;
}

#endif
