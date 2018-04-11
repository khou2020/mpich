#include <assert.h>
#include <stdarg.h>

#include "logfs.h"
#include "ad_logfs.h"
#include "ad_logfs_common.h"
#include "logfs_info.h"
#include "logfs_file.h"
#include "writering.h"
#include "rtree.h"
#include "adio_extern.h"
#include "typehelper.h"
#include "logfs_rtree.h"
#include "logfs_user.h"


#define LOGFS_LOCKFILE_MAGIC  "logfs-logfsfile\n"


/* Define if you want logfs to sync often */
/* #define LOGFS_DOSYNC */


/* structure for having a writering write to MPI file */
typedef struct {
    /* could also add hints / mode flags here */
    char *filename;
    MPI_File file;
    MPI_Request writereq;
    MPI_Request readreq;
    int readopen;               /* is file opened for reading&writing */
    int writeopen;
    MPI_Status status;
    unsigned int readsize;      /* size of active read request */
    unsigned int writesize;     /* size of active write request */
} writering_mpi_data;

#define LOGFS_FLAG_MODE_REPLAY       2
#define LOGFS_FLAG_MODE_ACTIVE       3
                                         /* real file is opened with active
                                          * logging */

typedef struct {
    char magic[64];             /* file magic */
    int flags;
    int logfilecount;           /* max. number of lock files possibly created for
                                 * this file (acros reopens) (== number of CPUs
                                 * used in open/create) */
    int epoch;                  /* next epoch number (used in reopen) */
    char logfilebase[255];      /* base filename for logfiles */
} logfs_logfsfile_header;


/* try to keep some statistics so that we can adapt
 * (eg upgrading to read from read_some if we miss too much reads) */
typedef struct {
    int rtree_miss;
    int rtree_hit;
    int rtree_overflow;
    int rtree_indep_flush;
} logfs_stats;

struct ADIO_LOGFS_Hints {
    int debug;                  /* output debug info */
    int readmode;               /* level of read support requested by the user */
    int datablocksize;          /* size of write combining buffer (data) */
    int datablockcount;         /* Number of write buffers (data) */
    int metablocksize;          /* size of buffer for metalog */
    int metablockcount;         /* number of buffers for metalog */
   int                  flushblocksize;     /* size of intermediate buffer when replaying log file */
    int sync;                   /* Don't do async write combining */
    char *logfilebase;          /* Basename/dir for logfiles */
    int replay_on_close;        /* replay when closing */
    int timereplay;             /* output replay timing */
};

struct ADIO_LOGFS_Data {

    struct ADIO_LOGFS_Hints hints;

    /* actual readmode */
    int readmode;

    /* used for writering */
    writering_handle writedata; /* for writing raw data */
    writering_handle writemeta; /* for writing metadata */
    writering_mpi_data writedata_state; /* info for writing to file */
    writering_mpi_data writemeta_state;

    logfs_file_handle logfsfile;
   char                 logfilebase[PATH_MAX];
    char *realfilename;

    /* logfsfile (handled by CPU 0) */
    logfs_logfsfile_header logfsfileheader;
    char *logfsfilename;
    MPI_File logfsfilehandle;

    /* lockfile (indicates logfs file is already open) */
    char *lockfilename;
    MPI_File lockfilehandle;

    MPI_Comm comm;              /* comm used for file */
    int commrank;               /* rank in comm */

    ADIO_Offset filesize;       /* current size of the file (or what this CPU thinks) */

    ADIO_Offset view_disp;
    MPI_Datatype view_etype;
    MPI_Datatype view_ftype;
    MPI_Aint view_ftype_extent;
    int view_ftype_size;
    int view_etype_size;


    logfs_rtree tree;
    int rtree_valid;            /* if the rtree is up to date */
    int file_valid;             /* If the real file is up to date */

    logfs_stats stats;


    /* file handles */
    MPI_File realfile_single;
    MPI_File realfile_collective;

    /* User replay */
    int user_replay;
    logfs_user_replay_cb user_replay_cb;

   int                  user_amode;
};

typedef struct ADIO_LOGFS_Data ADIO_LOGFS_Data;


/**************************************************************************/
/**************************************************************************/
/************************* forwards ***************************/
/**************************************************************/

static void logfs_replay_buildrtree(ADIO_LOGFS_Data * data, int all);
static inline void logfs_safeprefix(const char *name, char *dest, int size);
int logfs_replay_helper(ADIO_LOGFS_Data * data, int collective);
void logfs_transfer_hints(MPI_Info source, MPI_Info dest);
static int logfs_user_replay(ADIO_LOGFS_Data * data);

static void debuginfo(const char *str, ...);


static inline int checkError(int ret)
{
    char msg[MPI_MAX_ERROR_STRING];
    int resultlen;
    if (ret == MPI_SUCCESS)
        return 0;

    MPI_Error_string(ret, msg, &resultlen);
    debuginfo("%s\n", msg);
    // Disabled for indep build
    //MPL_backtrace_show(stderr);
    assert(0);
   return -1;
}

static void debuginfo(const char *str, ...)
{
    va_list list;
    va_start(list, str);
    fprintf(stderr, "logfs: ");
    vfprintf(stderr, str, list);
    va_end(list);
}

static const char *readmode2string(int readmode)
{
    switch (readmode) {
    case LOGFS_READMODE_NONE:
        return "readmode_none";
    case LOGFS_READMODE_SOME:
        return "readmode_some";
    case LOGFS_READMODE_PHASED:
        return "readmode_phased";
    case LOGFS_READMODE_FULL:
        return "readmode_full";
    default:
        return "(unknown readmode)";
    };
}

/****************************************************************************
 * writering writing to mpi files                                           *
 ****************************************************************************/
static int logfs_writering_mpi_init(void *opsdata, int read, int write)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    MPI_Info info = MPI_INFO_NULL;
    int flags;

    assert(data->filename);

    data->writereq = MPI_REQUEST_NULL;
    data->readreq = MPI_REQUEST_NULL;

    /* open file */
    assert(data->filename);

    checkError(MPI_Info_create(&info));
    /* todo: set some beter hints: mostly_writes, mostly_reads, ... */
    checkError(MPI_Info_set(info, "access_style", "sequential"));

    flags = MPI_MODE_UNIQUE_OPEN | MPI_MODE_CREATE;
   /* cannot use tighter permissions due to replay-on-close */
   flags |= MPI_MODE_RDWR;

    /* Are MPI_File functions reentrant?? */
    checkError(MPI_File_open(MPI_COMM_SELF, data->filename, flags, info, &data->file));

    checkError(MPI_Info_free(&info));

    data->writeopen = write;
    data->readopen = read;
    return 1;
}

static int logfs_writering_mpi_done(void *opsdata)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    assert(data);
    assert(MPI_REQUEST_NULL == data->writereq);
    assert(MPI_REQUEST_NULL == data->readreq);

    checkError(MPI_File_close(&data->file));

    return 1;
}

static int logfs_writering_mpi_start_write(void *opsdata, WRR_OFFSET ofs,
                                           const void *writedata, unsigned int size)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;

    assert(data);
    assert(data->writereq == MPI_REQUEST_NULL);

    checkError(MPI_File_iwrite_at(data->file, ofs, (void *) writedata, size,
                                  MPI_BYTE, &data->writereq));

    return 1;
}

static int logfs_writering_mpi_test_write(void *opsdata, unsigned int *written)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    MPI_Status stat;
    int flag;
    int w = 0;

    assert(data);
    assert(data->writereq != MPI_REQUEST_NULL);

    checkError(MPI_Test(&data->writereq, &flag, &stat));

    if (flag)
        checkError(MPI_Get_count(&stat, MPI_BYTE, &w));

    *written = (unsigned int) w;
    return flag;
}

static int logfs_writering_mpi_wait_write(void *opsdata, unsigned int *written)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    MPI_Status stat;
    int w;

    assert(data);
    assert(data->writereq != MPI_REQUEST_NULL);

    checkError(MPI_Wait(&data->writereq, &stat));
    checkError(MPI_Get_count(&stat, MPI_BYTE, &w));
    *written = (unsigned int) w;

    assert(data->writereq == MPI_REQUEST_NULL);

    return 1;
}

static int logfs_writering_mpi_flush(void *opsdata)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    assert(data);

    /* no flush with outstanding requests */
    assert(data->writereq == MPI_REQUEST_NULL);
    assert(data->readreq == MPI_REQUEST_NULL);

#ifdef LOGFS_DOSYNC
    /* flush logfile */
    checkError(MPI_File_sync(data->file));
#endif

    return 1;
}

static int logfs_writering_mpi_reset(void *opsdata, WRR_OFFSET ofs)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    assert(data);

    /* no reset with outstanding requests */
    assert(data->writereq == MPI_REQUEST_NULL);
    assert(data->readreq == MPI_REQUEST_NULL);

    /* truncate file */
    checkError(MPI_File_set_size(data->file, ofs));

    return 1;
}

static int logfs_writering_mpi_getsize(void *opsdata, WRR_OFFSET * ofs)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    MPI_Offset offset;

    assert(data);
    assert(data->file != MPI_FILE_NULL);

    checkError(MPI_File_get_size(data->file, &offset));
    *ofs = offset;

    return 1;
}



static int logfs_writering_mpi_start_read(void *opsdata, WRR_OFFSET ofs, void *readdata,
                                          unsigned int size)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;

    assert(MPI_FILE_NULL != data->file);

    assert(data->readreq == MPI_REQUEST_NULL);


    checkError(MPI_File_iread_at(data->file, ofs, readdata, size, MPI_BYTE, &data->readreq));

    return 1;
}

static int logfs_writering_mpi_wait_read(void *opsdata, unsigned int *size)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    MPI_Status stat;
    int count;

    assert(data);
    assert(size);
    assert(data->readreq != MPI_REQUEST_NULL);

    checkError(MPI_Wait(&data->readreq, &stat));
    assert(data->readreq == MPI_REQUEST_NULL);

    checkError(MPI_Get_elements(&stat, MPI_BYTE, &count));
    *size = count;
    return count;
}

static int logfs_writering_mpi_test_read(void *opsdata, unsigned int *size)
{
    writering_mpi_data *data = (writering_mpi_data *) opsdata;
    MPI_Status stat;
    int flag;
    int count;

    assert(size);
    assert(data->readreq != MPI_REQUEST_NULL);

    checkError(MPI_Test(&data->readreq, &flag, &stat));
    if (!flag)
        return flag;

    checkError(MPI_Get_elements(&stat, MPI_BYTE, &count));
    *size = count;
    return 1;
}

/***************************************************************************/
/***** logfs_file callbacks ************************************************/
/***************************************************************************/

/* logfs file also uses a callback mechanism to perform the actual
 * file I/O */

/* called the first time something meaningful is written to the log */
/* don't need to do anyhting; the writebuffers are already created */
static int logfs_file_cb_init(void *userdata)
{
    return 1;
}

static inline writering_handle logfs_file_cb_findhandle(void *userdata, int log)
{
    ADIO_LOGFS_Data *logfs = (ADIO_LOGFS_Data *) userdata;
    writering_handle handle = 0;
    switch (log) {
    case LOGFS_FILE_LOG_DATA:
        handle = logfs->writedata;
        break;
    case LOGFS_FILE_LOG_META:
        handle = logfs->writemeta;
        break;
    default:
        assert(0 /* UNKNOWN WRITE TYPE */);
    };
    return handle;
}

static int logfs_file_cb_write(void *userdata, ADIO_Offset ofs, const void *data, int size, int log)
{
    // ADIO_LOGFS_Data * logfs = (ADIO_LOGFS_Data *) userdata;
    writering_handle handle = logfs_file_cb_findhandle(userdata, log);

    writering_write(handle, ofs, data, size);
    return 1;
}

static int logfs_file_cb_done(void *userdata)
{
    return 1;
}

static int logfs_file_cb_restart(void *userdata, ADIO_Offset offset, int log)
{
    //ADIO_LOGFS_Data * logfs = (ADIO_LOGFS_Data *) userdata;
    writering_handle handle = logfs_file_cb_findhandle(userdata, log);
    writering_reset(handle, offset);
    return 1;
}

static int logfs_file_cb_getsize(void *userdata, ADIO_Offset * ofs, int log)
{
    //ADIO_LOGFS_Data * logfs = (ADIO_LOGFS_Data *) userdata;
    writering_handle handle = logfs_file_cb_findhandle(userdata, log);
    writering_getsize(handle, ofs);
    return 1;
}

static int logfs_file_cbr_init(void *userdata)
{
    return 1;
}

static int logfs_file_cbr_done(void *userdata)
{
    return 1;
}

static int logfs_file_cbr_read(void *userdata, ADIO_Offset ofs, void *data, int size, int log)
{
    //ADIO_LOGFS_Data * logfs = (ADIO_LOGFS_Data *) userdata;

    writering_handle handle = logfs_file_cb_findhandle(userdata, log);


    return writering_read(handle, ofs, data, size);
}

/***************************************************************************/
/***** Lock file ***********************************************************/
/***************************************************************************/

/* check if the lockfile exists */
static int logfs_lockfile_islocked(ADIO_LOGFS_Data * data)
{
    int ret;
    MPI_File file;

    ret = MPI_File_open(MPI_COMM_SELF, data->lockfilename, MPI_MODE_RDONLY, MPI_INFO_NULL, &file);
    if (ret != MPI_SUCCESS)
        return 0;

    MPI_File_close(&file);
    return 1;
}

static int logfs_lockfile_lock(ADIO_LOGFS_Data * data)
{
    if (!data->commrank) {
        int ret;
        assert(data->lockfilehandle == MPI_FILE_NULL);
        ret = MPI_File_open(MPI_COMM_SELF, data->lockfilename,
                            MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_EXCL |
                            MPI_MODE_DELETE_ON_CLOSE, MPI_INFO_NULL, &data->lockfilehandle);
        if (ret != MPI_SUCCESS)
            return 0;
    }
    checkError(MPI_Barrier(data->comm));
#ifndef NDEBUG
    assert(logfs_lockfile_islocked(data));
    checkError(MPI_Barrier(data->comm));
#endif
    return 1;
}

static int logfs_lockfile_unlock(ADIO_LOGFS_Data * data)
{
    if (!data->commrank) {
        /* delete on close should remove the file */
        checkError(MPI_File_close(&data->lockfilehandle));

        /*checkError(MPI_File_delete (data->lockfilename, MPI_INFO_NULL)); */
    }
    MPI_Barrier(data->comm);
#ifndef NDEBUG
    /* Check if DELETE_ON_CLOSE works
     * (of course, there is a slim chance that somebody else locked the file in
     * the mean time) */
    assert(!logfs_lockfile_islocked(data));

    /* 2nd barrier needed to avoid the one of our CPUs has locked the file
     * again in the mean time (which would cause the assert above to fail) */
    MPI_Barrier(data->comm);
#endif
    return 1;
}

static void logfs_logfsfile_update(ADIO_LOGFS_Data * data)
{
    if (data->commrank)
        return;

    assert(data->logfsfilehandle != MPI_FILE_NULL);

    checkError(MPI_File_write(data->logfsfilehandle, &data->logfsfileheader,
                              sizeof(data->logfsfileheader), MPI_BYTE, MPI_STATUS_IGNORE));
#ifdef LOGFS_DOSYNC
    checkError(MPI_File_sync(data->logfsfilehandle));
#endif
}


/* Read the header from an existing logfs file */
static int logfs_logfsfile_read(const char *filename, logfs_logfsfile_header * dest)
{
    logfs_logfsfile_header h;
    MPI_File file;
    int ret = 0;

    MPI_Status stat;

    ret = MPI_File_open(MPI_COMM_SELF, (char *) filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &file);

    if (ret != MPI_SUCCESS)
        return 0;

    /* read values and delete file */
    if (MPI_SUCCESS == MPI_File_read(file, &h, sizeof(h), MPI_BYTE, &stat)) {
        int ele;
        MPI_Get_elements(&stat, MPI_BYTE, &ele);
        if (ele == sizeof(h) && !strncmp(h.magic, LOGFS_LOCKFILE_MAGIC,
                                         strlen(LOGFS_LOCKFILE_MAGIC))) {
            /* everything OK */
            *dest = h;
            ret = 1;
        }
        else {
            /* Couldn't read enough from the file, or the magic
             * didn't work out*/
            ret = 0;
        }
    }
    else {
        /* error reading file */
        ret = 0;
    }

    MPI_File_close(&file);

    return ret;
}

/* try to open an existing logfile;
 * return true if an existing logfsfile was found;
 * Return false (and create a default one) otherwise
 */
static int logfs_logfsfile_create (ADIO_LOGFS_Data * data, int access_mode)
{
    int commsize;
    logfs_logfsfile_header h;
    int reopen = 0;

    MPI_Comm_size(data->comm, &commsize);

    /* CPU 0 tries to open the header */
    if (!data->commrank) {

        /* Create default header */
      memset(&h, 0, sizeof(h)); 
      strncpy (h.magic, LOGFS_LOCKFILE_MAGIC, sizeof(h.magic)); 
        h.flags = LOGFS_FLAG_MODE_ACTIVE;
        h.logfilecount = commsize;
        h.epoch = 0;
        memset(&h.logfilebase[0], 0, sizeof(h.logfilebase));
      if (data->logfilebase != NULL)
	  strncpy (h.logfilebase, data->logfilebase, sizeof(h.logfilebase));

        /* see if we can open an existing logfs file */
        assert(data->logfsfilehandle == MPI_FILE_NULL);
        if (logfs_logfsfile_read(data->logfsfilename, &h)) {
            /* Could open existing file; Load the old values */
            data->logfsfileheader = h;

            /* Increase the epoch */
            ++data->logfsfileheader.epoch;

            if (data->logfsfileheader.logfilecount != commsize) {
                fprintf(stderr, "logfs: Error: Cannot use %i CPUs to "
                        "open logfs file %s, created on %i CPUs!\n",
                        commsize, data->logfsfilename, data->logfsfileheader.logfilecount);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            reopen = 1;
        }
        else {
            /* Use defaults for new file */
            data->logfsfileheader = h;

            reopen = 0;
        }

      /* Open file for writing.  Replay-on-close might require reading the
       * header, so we cannot open write-only  */
        checkError(MPI_File_open(MPI_COMM_SELF, data->logfsfilename,
               MPI_MODE_CREATE|MPI_MODE_RDWR,
                                 MPI_INFO_NULL, &data->logfsfilehandle));
        logfs_logfsfile_update(data);
    }

    MPI_Bcast(&data->logfsfileheader, sizeof(data->logfsfileheader), MPI_BYTE, 0, data->comm);

    MPI_Barrier(data->comm);
    return reopen;
}


static void logfs_logfsfile_remove(ADIO_LOGFS_Data * data)
{
    MPI_Barrier(data->comm);

    if (!data->commrank) {
        assert(data->logfsfilehandle != MPI_FILE_NULL);
        MPI_File_close(&data->logfsfilehandle);
        MPI_File_delete(data->logfsfilename, MPI_INFO_NULL);
    }

    MPI_Barrier(data->comm);
}


/***************************************************************************/
/***************************************************************************/
/***************************************************************************/


/* Create lockfilename given the real filename */
static void logfs_lockfilename(const char *filename, char *buf, int bufsize)
{
   char buf2[PATH_MAX];
    logfs_safeprefix(filename, buf2, sizeof(buf2));

    logfs_safeprefix(filename, buf2, sizeof(buf2));
    snprintf(buf, bufsize, "%s.logfslock", buf2);
}

/* create logfsfilename give the real filename; */
static void logfs_logfsfilename(const char *filename, char *buf, int bufsize)
{
   char buf2[PATH_MAX];

    logfs_safeprefix(filename, buf2, sizeof(buf2));
    snprintf(buf, bufsize, "%s.logfs", buf2);
}

static int logfs_logfilename(const char *logfilebase, char *buf, int bufsize,
                             int cpunum, int logtype)
{
   snprintf (buf, bufsize, "%s.%u.%s", logfilebase, cpunum,
                   (logtype == LOGFS_FILE_LOG_META ? "meta" : "data"));
    return 1;
}

/* find out if this file is a logfs file */
/* reentry problem here in ADIO functions? */
int logfs_probe(MPI_Comm comm, const char *filename)
{
    char buf[255];
    MPI_File handle;
    int rank;
    int ret;

    MPI_Comm_rank(comm, &rank);

    if (!rank) {
        logfs_logfsfilename(filename, buf, sizeof(buf) - 1);

        /* try to open logfsfile */
        if (MPI_SUCCESS == MPI_File_open(MPI_COMM_SELF, buf, MPI_MODE_RDONLY,
                                         MPI_INFO_NULL, &handle)) {
            MPI_File_close(&handle);
            ret = 1;
        }
    }

    MPI_Bcast(&ret, 1, MPI_INT, 0, comm);
    return ret;
}

/* called by the MPI-IO delete function when a logfs file is detected
 * Only deletes the .meta, .data, and .logfs files */
int logfs_delete (const char * filename)
{
    char buf[255];
    MPI_File handle;
    logfs_logfsfile_header header;
    int i;
    int err;
    MPI_Status status;

    /* see if the file is locked */
    /* TODO */

    /* open logfsfile; get logfile basename & count; remove */
    logfs_logfsfilename(filename, buf, sizeof(buf) - 1);

    err = MPI_File_open(MPI_COMM_SELF, buf, MPI_MODE_RDONLY, MPI_INFO_NULL, &handle);

    /* if the logfsfile doesn't exist, no prob; just remove the real file */
    if (err != MPI_SUCCESS)
        return 0;

    MPI_File_read(handle, &header, sizeof(header), MPI_BYTE, &status);
    MPI_Get_count(&status, MPI_BYTE, &err);

    MPI_File_close(&handle);

    if (err != sizeof(header) ||
        (strncmp(header.magic, LOGFS_LOCKFILE_MAGIC, strlen(LOGFS_LOCKFILE_MAGIC)))) {
        debuginfo("MPI_File_delete: %s; no valid logfsfile found!"
                  "\nNot trying to delete logfsfile/logfiles\n", filename);
        return 0;
    }

    /* don't delete an open file! */
    /* not active for now */
/*   assert (!(header.flags & LOGFS_FLAG_MODE_ACTIVE));  */
    /* delete logfsfile */
    MPI_File_delete(buf, MPI_INFO_NULL);

    /* MPI_File_delete shouldn't be called on every CPU,
     * so the one volunteer has to remove all of the logfiles
     * (manot always possible);
     * This will not work out if there are different logbases
     * on different CPUs (or the same logbase but on different filesystems) */
    for (i = 0; i < header.logfilecount; ++i) {
        /* generate logfilename and remove */
        logfs_logfilename(header.logfilebase, buf, sizeof(buf) - 1, i, LOGFS_FILE_LOG_META);
        MPI_File_delete(buf, MPI_INFO_NULL);

        /* same for data log */
        logfs_logfilename(header.logfilebase, buf, sizeof(buf) - 1, i, LOGFS_FILE_LOG_DATA);
        MPI_File_delete(buf, MPI_INFO_NULL);
    }
    return 1;
}

/* by design, always true */
int logfs_active(ADIO_File fd)
{
    //ADIO_LOGFS_Data * data = logfs_data (fd);
    return 1;
}

/* =================== flushing tree to disk ======================== */
typedef struct {
    MPI_File datalog;
    MPI_Request readreq;
    MPI_Request writereq;
    MPI_Info readinfo;
    MPI_Info writeinfo;
    int collective;
    ADIO_LOGFS_Data *logfsdata;
} logfs_flushtree_state;

static int logfs_flush_start(void *userdata, int coll)
{
    logfs_flushtree_state *state = (logfs_flushtree_state *) userdata;
    state->readreq = MPI_REQUEST_NULL;
    state->writereq = MPI_REQUEST_NULL;
    state->collective = coll;
    MPI_Info_create(&state->readinfo);
    MPI_Info_set(state->readinfo, "access_style", "read_once,sequential");
    MPI_Info_create(&state->writeinfo);
    MPI_Info_set(state->writeinfo, "access_style", "write_once,sequential");

    /* make sure datalog is flushed  (since we read directly from the disk) */
    writering_flush(state->logfsdata->writedata);

    return 1;
}

static int logfs_flush_stop(void *userdata)
{
    logfs_flushtree_state *state = (logfs_flushtree_state *) userdata;
    assert(state->readreq == MPI_REQUEST_NULL);
   assert (state->writereq == MPI_REQUEST_NULL); 
    MPI_Info_free(&state->readinfo);
    MPI_Info_free(&state->writeinfo);
    return 1;
}

static int logfs_flush_readstart(void *buf, MPI_Datatype memtype, MPI_Datatype filetype,
                                 void *userdata)
{
    //MPI_Info info;
    int err;
    logfs_flushtree_state *state = (logfs_flushtree_state *) userdata;
    assert(state->readreq == MPI_REQUEST_NULL);


    /* TODO: create better info (such as mostly-sequential, readonce, ...) */
    err = MPI_File_set_view(state->datalog, 0, MPI_BYTE, filetype, "native", MPI_INFO_NULL);
    assert(MPI_SUCCESS == err);
    err = MPI_File_iread_at(state->datalog, 0, buf, 1, memtype, &state->readreq);
    assert(MPI_SUCCESS == err);

    /* restore view; we are using the same file handle as the writering is */
    err = MPI_File_set_view(state->datalog, 0, MPI_BYTE, MPI_BYTE, "native", MPI_INFO_NULL);
    assert(MPI_SUCCESS == err);
    return 1;
}

static int logfs_flush_readwait(void *userdata)
{
    logfs_flushtree_state *state = (logfs_flushtree_state *) userdata;
    MPI_Wait(&state->readreq, MPI_STATUS_IGNORE);
    return 1;
}

/* Remove logfs: prefix if any to avoid recursion */
static inline void logfs_safeprefix(const char *name, char *dest, int size)
{
    if (strncmp(name, "logfs:", 6) != 0)
        ADIOI_Strncpy(dest, name, size);
    else
        ADIOI_Strncpy(dest, name + 6, size);
}

static int logfs_ensureopen (ADIO_LOGFS_Data * data,
	MPI_Info info, int collective)
{
    int err;
    char buf[255];
    if (collective) {
        if (MPI_FILE_NULL != data->realfile_collective)
         return -1;
        logfs_safeprefix(data->realfilename, buf, sizeof(buf));
      err=MPI_File_open (data->comm, buf, data->user_amode,
                            info, &data->realfile_collective);
      if (err != MPI_SUCCESS) goto fn_exit;

    }
    if (MPI_FILE_NULL != data->realfile_single)
	return -1;
    /* think about consistency in this case; probably need to flush these
     * too when the user calls sync */
    logfs_safeprefix(data->realfilename, buf, sizeof(buf));
    err = MPI_File_open(MPI_COMM_SELF, buf,
	    MPI_MODE_RDWR|MPI_MODE_CREATE,
	    info, &data->realfile_single);
fn_exit:
   return err;
}

/* for writes we assume that the memory buffer is continuous and in order
 * However we need to set the filetype which is collective! */
static int logfs_flush_writestart(void *buf, MPI_Datatype filetype, int bytes, void *userdata)
{
    logfs_flushtree_state *state = (logfs_flushtree_state *) userdata;
    MPI_File handle = (state->collective ? state->logfsdata->realfile_collective :
                       state->logfsdata->realfile_single);
    assert(state->writereq == MPI_REQUEST_NULL);

    if (MPI_FILE_NULL == handle) {
        logfs_ensureopen(state->logfsdata, state->writeinfo, state->collective);
        handle = (state->collective ? state->logfsdata->realfile_collective :
                  state->logfsdata->realfile_single);
        assert(MPI_FILE_NULL != handle);
    }

   checkError(MPI_File_set_view (handle, 0, MPI_BYTE, filetype,
	       "native", state->writeinfo));
   checkError(MPI_File_iwrite_at (handle, 0, buf, bytes,
	       MPI_BYTE, &state->writereq));

    return 1;
}

static int logfs_flush_writewait(void *userdata)
{
    logfs_flushtree_state *state = (logfs_flushtree_state *) userdata;
    MPI_Wait(&state->writereq, MPI_STATUS_IGNORE);
    return 1;
}

static void logfs_flushtree(ADIO_LOGFS_Data * data, rtree_const_handle tree, int collective)
{
    ADIO_Offset filesize = 0;
   int flushsize;
    logfs_flushtree_state state;
    logfs_rtree_flush_cb cb;

    cb.start = logfs_flush_start;
    cb.stop = logfs_flush_stop;
    cb.readwait = logfs_flush_readwait;
    cb.readstart = logfs_flush_readstart;
    cb.writestart = logfs_flush_writestart;
    cb.writewait = logfs_flush_writewait;

    state.datalog = data->writedata_state.file;
    state.readreq = state.writereq = MPI_REQUEST_NULL;
    state.readinfo = MPI_INFO_NULL;
    state.writeinfo = MPI_INFO_NULL;
    state.collective = collective;
    state.logfsdata = data;

   if (data->hints.flushblocksize != 0)
       flushsize = data->hints.flushblocksize;
   else
       flushsize = 1024*1024;

   logfs_rtree_flush (&data->tree, flushsize,
	 &cb, &state, collective, &filesize, data->comm);
}

int logfs_replay_helper(ADIO_LOGFS_Data * data, int collective)
{
    double start = MPI_Wtime();
    double stop;

    if (!data->user_replay) {
        /* Needed in case file is open in wr-only mode, there are pending
         * writes (in the background) and we want to read from the file;
         * The read causes an reopen into rw-mode, but cannot do this because of
         * an active write request... Flushing first avoids active writes */

        writering_flush(data->writedata);
        writering_flush(data->writemeta);

        data->rtree_valid = 0;
        if (!data->rtree_valid) {
            /* if there is no valid rtree, build the tree (all epochs) */
            /* (we need a full replay because we are also forced to return
             * data that other CPUs wrote in older epochs...) */
            logfs_replay_buildrtree(data, 1);
        }

        /* dump tree to disk (collective) */
        logfs_flushtree(data, data->tree.rtree, collective);
    }
    else {
        logfs_user_replay(data);
    }

    /* truncate datalogfiles if they aren't empty already */
    /* also empty the tree */
    if (!rtree_empty(data->tree.rtree)) {
        logfs_file_clear(data->logfsfile, 0);
        rtree_clear(data->tree.rtree);
        data->tree.rangesize = 0;
    }

    stop = MPI_Wtime();
    if (data->hints.debug || data->hints.timereplay) {
        debuginfo("Replay: start,stop = %f,%f\n", start, stop);

        if ((stop - start) > 0.001) {
            debuginfo("Replay (collective=%u) of %lu bytes took: %f (%f MB/s)\n",
                      (unsigned) collective,
                      (long unsigned) data->tree.rangesize, stop - start,
                      (double) data->tree.rangesize / ((stop - start) * 1024 * 1024));
        }
        else {
            debuginfo("Replay (collective=%u) of %lu bytes took: %f\n",
                      collective, data->tree.rangesize, stop - start);
        }
    }

    /* Did a full replay, so the real file is now valid */
    data->file_valid = 1;
    return 0;
}

/* called when the WHOLE file needs to be replayed;
 * collective */
int logfs_replay(ADIO_File fd, int collective)
{

    ADIO_LOGFS_Data *data = logfs_data(fd);

    data->logfsfileheader.flags |= LOGFS_FLAG_MODE_REPLAY;
    logfs_logfsfile_update(data);

    logfs_replay_helper(data, collective);
    data->file_valid = 1;

    data->logfsfileheader.flags ^= ~LOGFS_FLAG_MODE_REPLAY;
    logfs_logfsfile_update(data);

    return 1;
}


/* parse hints; get blocksize, blockcount & logfilebase */
static inline void logfs_activate_initwritering(ADIO_File fd,
                                                ADIO_LOGFS_Data * data, MPI_Info info, int reopen)
{
    writering_ops ops;
    char buf[255];
    int read;
    int write;

    /* find out if we're readonly, write only or readwrite */
    if (fd->access_mode & ADIO_WRONLY) {
        read = 0;
        write = 1;
    }
    else if (fd->access_mode & ADIO_RDONLY) {
        /* we set write to 1 so that the file can truncated/removed after
         * replay */
        read = 1;
        write = 1;
    }
    else if (fd->access_mode & ADIO_RDWR) {
        read = 1;
        write = 1;
    }
    else
        debuginfo("invalid RD/WR flags in logfs_activate??\n");

    /* defaults are in hints  */
   data->logfilebase[0] = 0;

    /* first check hint */
   if (data->hints.logfilebase)
      ADIOI_Strncpy(data->logfilebase, data->hints.logfilebase, PATH_MAX);

    /* check environment (only if no hint is set) */
   if (getenv ("LOGFSTMP"))
      ADIOI_Strncpy(data->logfilebase, getenv("LOGFSTMP"), PATH_MAX);

    /* if no logfilebase was set, and the user indicated no preference,
     * put it next to the real file */
   if (data->logfilebase[0] == 0)
   {
        char buf[255];
        logfs_safeprefix(data->realfilename, buf, sizeof(buf));
      ADIOI_Strncpy(data->logfilebase, buf, PATH_MAX);
    }

    assert(data->logfilebase);

    /* if nothing specified, choose 2 1MB for data buffers */
    if (!data->hints.datablockcount || !data->hints.datablocksize) {
        data->hints.datablockcount = 2;
        data->hints.datablocksize = 4 * 1024 * 1024;
    }

    /* if nothing given, choose 2 64k buffers for metadata */
    if (!data->hints.metablockcount || !data->hints.metablocksize) {
        data->hints.metablockcount = 2;
        data->hints.metablocksize = 64 * 1024;
    }


    ops.start_write = logfs_writering_mpi_start_write;
    ops.test_write = logfs_writering_mpi_test_write;
    ops.wait_write = logfs_writering_mpi_wait_write;
    ops.flush = logfs_writering_mpi_flush;
    ops.init = logfs_writering_mpi_init;
    ops.done = logfs_writering_mpi_done;
    ops.getsize = logfs_writering_mpi_getsize;
    ops.reset = logfs_writering_mpi_reset;
    ops.start_read = logfs_writering_mpi_start_read;
    ops.test_read = logfs_writering_mpi_test_read;
    ops.wait_read = logfs_writering_mpi_wait_read;

    /* fill in state for datalog en metadata log files */
    logfs_logfilename(data->logfilebase, buf, sizeof(buf) - 1, data->commrank, LOGFS_FILE_LOG_DATA);
    data->writedata_state.filename = ADIOI_Strdup(buf);
    data->writedata_state.file = MPI_FILE_NULL;
    data->writedata_state.readopen = 0;
    data->writedata_state.writeopen = 0;
    data->writedata_state.readreq = data->writedata_state.writereq = MPI_REQUEST_NULL;

    logfs_logfilename(data->logfilebase, buf, sizeof(buf) - 1, data->commrank, LOGFS_FILE_LOG_META);
    data->writemeta_state.filename = ADIOI_Strdup(buf);
    data->writemeta_state.file = MPI_FILE_NULL;
    data->writemeta_state.readopen = 0;
    data->writemeta_state.writeopen = 0;
    data->writemeta_state.readreq = data->writemeta_state.writereq = MPI_REQUEST_NULL;

    /* create the write buffers; They do not allocate memory/open the files until the
     * first read/write anyway */
    data->writedata = writering_create(data->hints.datablocksize, data->hints.datablockcount,
                                       &ops, &data->writedata_state, read, write);

    data->writemeta = writering_create(data->hints.metablocksize, data->hints.metablockcount,
                                       &ops, &data->writemeta_state, read, write);

    data->realfile_single = MPI_FILE_NULL;
    data->realfile_collective = MPI_FILE_NULL;

    writering_setsync(data->writedata, data->hints.sync);
    writering_setsync(data->writemeta, data->hints.sync);

    if (data->hints.debug)
        debuginfo("init writering: meta: %i x %i bytes, data: %i x %i bytes, sync=%i\n",
                  data->hints.metablockcount, data->hints.metablocksize, data->hints.datablockcount,
                  data->hints.datablocksize, data->hints.sync);
}

static inline void logfs_activate_initlogfs(ADIO_LOGFS_Data * data, int reopen)
{
    logfs_file_ops logfsops;
    logfs_file_readops logfsreadops;

    /* init logfs_file */
    logfsops.done = logfs_file_cb_done;
    logfsops.init = logfs_file_cb_init;
    logfsops.write = logfs_file_cb_write;
    logfsops.restart = logfs_file_cb_restart;
    logfsops.getsize = logfs_file_cb_getsize;

    logfsreadops.init = logfs_file_cbr_init;
    logfsreadops.read = logfs_file_cbr_read;
    logfsreadops.done = logfs_file_cbr_done;

    /* if write_mostly is set, set to read_some else if read is set set
     * to read else read_none (talking about access style for logfiles)*/
    data->logfsfile = logfs_file_create(data->comm, &logfsops, data, &logfsreadops, data);
}


static void logfs_hints_default(struct ADIO_LOGFS_Hints *h)
{
    /* hints default */
    h->debug = 0;
    if (getenv("LOGFS_DEBUG"))
        h->debug = 1;
    h->sync = 0;
    h->metablockcount = 0;
    h->metablocksize = 0;
    h->datablockcount = 0;
    h->datablocksize = 0;
   h->flushblocksize = 0;
    h->readmode = LOGFS_READMODE_SOME;
    h->logfilebase = 0;
    h->replay_on_close = 0;
    h->timereplay = 1;
}


/*
 * can adjust:
 *    debug
 *    readmode (only upgrade between the different modes)
 *
 * - Can cause invalidation of tree
 * - Note that some hints only take affect at open time
 */
static void logfs_process_info(struct ADIO_LOGFS_Hints *hints, MPI_Info info)
{
    char *ptr = 0;

    /* Override timereplay from env */
    if (getenv("LOGFS_TIMEREPLAY"))
        hints->timereplay = 1;


    if (info == MPI_INFO_NULL)
        return;

    ad_logfs_hint_bool(info, LOGFS_INFO_DEBUG, &hints->debug);

   ad_logfs_hint_int (info, LOGFS_INFO_DATABLOCKCOUNT,
         &hints->datablockcount);
   ad_logfs_hint_int (info, LOGFS_INFO_DATABLOCKSIZE,
         &hints->datablocksize);
   ad_logfs_hint_int (info, LOGFS_INFO_METABLOCKCOUNT,
         &hints->metablockcount);
   ad_logfs_hint_int (info, LOGFS_INFO_METABLOCKSIZE,
         &hints->metablocksize);
   ad_logfs_hint_int(info, LOGFS_INFO_FLUSHBLOCKSIZE,
	   &hints->flushblocksize);
   ad_logfs_hint_bool (info, LOGFS_INFO_SYNC,
         &hints->sync);
   ad_logfs_hint_str (info, LOGFS_INFO_LOGBASE,
         &hints->logfilebase);

   ad_logfs_hint_bool (info, LOGFS_INFO_REPLAYCLOSE,
         &hints->replay_on_close);

   ad_logfs_hint_bool (info, LOGFS_INFO_TIMEREPLAY,
         &hints->timereplay);

    /* Override timereplay from env */
    if (getenv("LOGFS_TIMEREPLAY"))
        hints->timereplay = 1;


    ad_logfs_hint_str(info, LOGFS_INFO_READMODE, &ptr);
    hints->readmode = 0;
    if (ptr) {
        if (!strcmp(ptr, "track_none"))
            hints->readmode = LOGFS_READMODE_NONE;
        else if (!strcmp(ptr, "track_some"))
            hints->readmode = LOGFS_READMODE_SOME;
        else if (!strcmp(ptr, "track_phased"))
            hints->readmode = LOGFS_READMODE_PHASED;
        else if (!strcmp(ptr, "track_all"))
            hints->readmode = LOGFS_READMODE_FULL;
        else if (hints->debug)
            debuginfo("logfs: unknown read mode (%s) requested in hint "
                      "(%s)!\n", ptr, LOGFS_INFO_READMODE);
      ADIOI_Free(ptr);
    }

}

/* modify given hint object so that it contains all the info
 * from the hints structure */
static void logfs_store_info(struct ADIO_LOGFS_Hints *hints, MPI_Info info)
{
    const char *ptr;
    assert(info != MPI_INFO_NULL);

    ad_logfs_hint_set_bool(info, LOGFS_INFO_DEBUG, hints->debug);

    switch (hints->readmode) {
    case LOGFS_READMODE_NONE:
        ptr = "track_none";
        break;
    case LOGFS_READMODE_SOME:
        ptr = "track_some";
        break;
    case LOGFS_READMODE_PHASED:
        ptr = "track_phased";
        break;
    case LOGFS_READMODE_FULL:
        ptr = "track_all";
        break;
    default:
        ptr = "unknown!";
        debuginfo("logfs: uknown read mode (%i) in hints->readmode\n", hints->readmode);
    }
    ad_logfs_hint_set_str(info, LOGFS_INFO_READMODE, ptr);
    ad_logfs_hint_set_int(info, LOGFS_INFO_DATABLOCKCOUNT, hints->datablockcount);
    ad_logfs_hint_set_int(info, LOGFS_INFO_DATABLOCKSIZE, hints->datablocksize);
    ad_logfs_hint_set_int(info, LOGFS_INFO_METABLOCKCOUNT, hints->metablockcount);
    ad_logfs_hint_set_int(info, LOGFS_INFO_METABLOCKSIZE, hints->metablocksize);

   ad_logfs_hint_set_int(info, LOGFS_INFO_FLUSHBLOCKSIZE,
	   hints->flushblocksize);
    ad_logfs_hint_set_bool(info, LOGFS_INFO_SYNC, hints->sync);
    ad_logfs_hint_set_str(info, LOGFS_INFO_LOGBASE, hints->logfilebase);
    ad_logfs_hint_set_bool(info, LOGFS_INFO_REPLAYCLOSE, hints->replay_on_close);
    ad_logfs_hint_set_bool(info, LOGFS_INFO_TIMEREPLAY, hints->timereplay);
}

void logfs_transfer_hints(MPI_Info source, MPI_Info dest)
{
    struct ADIO_LOGFS_Hints h;

    logfs_hints_default(&h);

    logfs_process_info(&h, source);
    logfs_store_info(&h, dest);
   if (h.logfilebase) ADIOI_Free(h.logfilebase);
}

/* update internal hints structure, also update fd->info
 * so that MPI_File_get_info returns correct values */
void logfs_setinfo(ADIO_File fd, MPI_Info info)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);
    logfs_process_info(&data->hints, info);
    logfs_store_info(&data->hints, fd->info);
}


/*
 * Lockfile:
 *   On activate, we need to:
 *      * Check if a lockfile exists
 *           * if so: file is already open -> error
 *      * check if a logfsfile already exists
 *           * if so it cannot have an ACTIVE state (this would mean the file
 *             is alreay open somewhere else)
 *           * If not, we need to load the epoch number from the header
 *             and append to the logfiles; Also, the number of CPUs should
 *             match and every CPU should be able to open its personal
 *             datalog&metalog
 *       * If the logfsfile doesn't exist, create one
 *
 * returns 1 if successful.  If not successful, populates error_code suitable
 * for handing back up to higher-level ROMIO
 */
int logfs_activate(ADIO_File fd, MPI_Info info)
{
    int error_code;
    char buf[255];
    const char *prefix;
    const int standalone = logfs_standalone(fd);
    int locked;
    int reopen = 0;
   int                  ret = 1;

    ADIO_LOGFS_Data *data = (ADIO_LOGFS_Data *)
      ADIOI_Calloc (1, sizeof(ADIO_LOGFS_Data));

    /* set default hints */
    logfs_hints_default(&data->hints);

    /* process hints */
    logfs_process_info(&data->hints, info);


    /* read mode:
     *   0: write-only
     *   1: reading allowed, but not expected (no write tracking)
     *   2: full write tracking
     */
    data->readmode = ((fd->access_mode & (ADIO_RDONLY | ADIO_RDWR)) ?
                      LOGFS_READMODE_SOME : LOGFS_READMODE_NONE);

    /* if readmode is enabled, and the user wants a different mode (selected
     * through hints) adjust */
    if ((data->readmode > LOGFS_READMODE_NONE) && (data->hints.readmode > LOGFS_READMODE_NONE))
        data->readmode = data->hints.readmode;

    if (data->hints.debug) {
        debuginfo("readmode is %s\n", readmode2string(data->readmode));
    }

    /* setup comm */
    MPI_Comm_dup(fd->comm, &data->comm);
    MPI_Comm_rank(data->comm, &data->commrank);


   /*======================================================
    * Determine filenames                                 *
    *======================================================*/

    /* get real filename (= filename without logfs prefix,
     * but including the native filesystem prefix (e.g. pvfs2:) */
    prefix = ADIO_FileTypeToPrefix(fd->file_system);
    assert(prefix != 0);
    data->realfilename = ADIOI_Malloc(strlen(prefix) + strlen(fd->filename) + 1);
    sprintf(data->realfilename, "%s%s", prefix, fd->filename);

   /* basedir */
   if (data->hints.logfilebase != NULL)
       ADIOI_Strncpy(data->logfilebase, data->hints.logfilebase, PATH_MAX );

    /* store logfsfilename (including prefix!) */
    logfs_logfsfilename(data->realfilename, buf, sizeof(buf) - 1);
    data->logfsfilename = ADIOI_Strdup(buf);
    data->logfsfilehandle = MPI_FILE_NULL;

    /* Store lockfilename (including prefix) */
    logfs_lockfilename(data->realfilename, buf, sizeof(buf) - 1);
    data->lockfilename = ADIOI_Strdup(buf);
    data->lockfilehandle = MPI_FILE_NULL;

    /* Before doing anything else, see if we can lock the file */
    locked = logfs_lockfile_lock(data);

    if (!locked) {
        /* No problem signaling the error/lock failure, but the cleanup
         * is messy ;-) So just give up for now */
        fprintf(stderr, "File %s is already opened using logfs!\n"
                "(If you're __sure__ it's not opened somewhere else,"
                " the lockfile (%s) might be stale; try removing it first\n",
                data->realfilename, data->lockfilename);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* See if the logfs file exists */
   reopen = logfs_logfsfile_create (data, fd->access_mode); 

    if (data->hints.debug) {
        if (!reopen)
            debuginfo("Creating new file...\n");
        else
            debuginfo("Reopening existing file...\n");
    }

    /* init logfiles */
    logfs_activate_initwritering(fd, data, info, reopen);
    logfs_activate_initlogfs(data, reopen);

    /* logfsfile create reads the epoch number if the file already exists;
     * pass this info to the logfs_file */
    logfs_file_setepoch(data->logfsfile, data->logfsfileheader.epoch);


    /* we expect a call to set the view after activating logfs */
    data->view_disp = 0;
    data->view_etype = MPI_DATATYPE_NULL;
    data->view_ftype = MPI_DATATYPE_NULL;

    /* an empty tree is up to date */
    data->rtree_valid = 1;

   data->user_amode = fd->access_mode;


    if (standalone) {
        fd->fs_ptr = data;

      ret = logfs_ensureopen (data, MPI_INFO_NULL, 1);
      if (ret != MPI_SUCCESS) {
	  logfs_deactivate(fd);
	  goto fn_exit;
      }
        MPI_File_get_size(data->realfile_collective, &data->filesize);
    }
    else {
      /** get some initial state */
        if (!data->commrank) {

            ADIO_Fcntl_t f;
            ADIO_Fcntl(fd, ADIO_FCNTL_GET_FSIZE, &f, &error_code);
            assert(MPI_SUCCESS == error_code);
            data->filesize = f.fsize;
        }
        MPI_Bcast(&data->filesize, 1, ADIO_OFFSET, 0, data->comm);      /* init layering */

        /* no more open required since our slave is already open */
        ADIOI_Layer_init(fd, &ADIO_LOGFS_operations, data, &error_code, 1);
    }


    /* view will be set later */

    /* create rtree */
    data->tree.rtree = rtree_create();
    data->tree.rangesize = 0;

    /* Assume real file is not valid until the first replay */
    data->file_valid = 0;
    data->user_replay = 0;
fn_exit:

   return ret; 

}

/* Remove the logfiles of this CPU; Should only be called with the logfile
 * handles closed */
static void logfs_logfiles_remove(ADIO_LOGFS_Data * data)
{
    assert(data->writedata_state.file == MPI_FILE_NULL);
    assert(data->writemeta_state.file == MPI_FILE_NULL);

    assert(data->writemeta_state.filename);
    assert(data->writedata_state.filename);

    MPI_File_delete(data->writemeta_state.filename, MPI_INFO_NULL);
    MPI_File_delete(data->writedata_state.filename, MPI_INFO_NULL);
}

int logfs_deactivate(ADIO_File fd)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);
    int standalone = logfs_standalone(fd);
    int replay = 0;

    /* File should be locked! */
    assert(data->commrank || data->lockfilehandle != MPI_FILE_NULL);

    if (data->hints.replay_on_close)
        replay = 1;

    assert(logfs_active(fd));
    if (data->hints.debug)
        debuginfo("Deactivating logfs (replay=%u) on %s\n", replay, fd->filename);

   /* collective replay: no need to replay if no writes */
   if (replay && !(data->user_amode & MPI_MODE_RDONLY))
   {
        logfs_replay(fd, 1);
    }


    /* mark file as no longer active */
    data->logfsfileheader.flags &= ~LOGFS_FLAG_MODE_ACTIVE;

    /* if we do a replay, no need to keep the trace files */
    if (replay)
        logfs_logfsfile_remove(data);
    else
        logfs_logfsfile_update(data);

    /* close logfs file */
    logfs_file_free(&data->logfsfile);

    /* close write buffers */
    /* writering calls writeops done (logfs_writering_mpi_done)
     * which closes the file */
    writering_free(&data->writedata);
    writering_free(&data->writemeta);

    /* have to keep this for last since logfs_data cannot be called anylonger
     * after the layering is deactivated */
    if (!standalone)
        ADIOI_Layer_done(fd);



    if (data->view_etype != MPI_DATATYPE_NULL)
        MPI_Type_free(&data->view_etype);
    if (MPI_DATATYPE_NULL != data->view_ftype)
        MPI_Type_free(&data->view_ftype);

    /* free rtree */
    rtree_free(&data->tree.rtree);
    data->tree.rangesize = 0;

    /* close files */
    if (data->realfile_single != MPI_FILE_NULL)
        MPI_File_close(&data->realfile_single);
    if (data->realfile_collective != MPI_FILE_NULL)
        MPI_File_close(&data->realfile_collective);
    if (data->logfsfilehandle != MPI_FILE_NULL)
        MPI_File_close(&data->logfsfilehandle);


    if (replay) {
        /* since we did a full replay, remove the per-cpu logfiles */
        logfs_logfiles_remove(data);
    }

    /* Finally, unlock the file */
    logfs_lockfile_unlock(data);

    /* Free strings */
   ADIOI_Free (data->writemeta_state.filename); 
   ADIOI_Free (data->writedata_state.filename); 
    ADIOI_Free(data->logfsfilename);
    ADIOI_Free(data->lockfilename);
    ADIOI_Free(data->realfilename);

    /* free logfilebase hint if any */
    if (data->hints.logfilebase)
        ADIOI_Free(data->hints.logfilebase);

    /* Free communicator */
    MPI_Comm_free(&data->comm);

    ADIOI_Free(data);
    return 1;
}


typedef struct {
    logfs_rtree *tree;
    ADIO_Offset datalogstart;
} logfs_processtypes_info;

/* callback for processtypes that just adds the region to the tree
 * The userdata should point to a logfs_processtypes_structure */
static int logfs_processtypes_addtree(void *membuf, int size, ADIO_Offset fileofs, void *data)
{
    logfs_processtypes_info *info = data;

    logfs_rtree_addsplit(info->tree, fileofs, fileofs + size, info->datalogstart);
    info->datalogstart += size;
    return 1;
}


/*
 * Look at the write datatype and calculate which parts of the file will be
 * affected; Optionally update the rtree or keep track of the filesize
 */
static void logfs_trackwrite(ADIO_LOGFS_Data * data,
                             MPI_Datatype memtype, int count, ADIO_Offset offset,
                             int updatetree, int tracksize, ADIO_Offset datalogstart)
{
    ADIO_Offset lastofs;
    int memtypesize;

    /* shouldn't call this if no work is to be done */
    assert(updatetree || tracksize);

    MPI_Type_size(memtype, &memtypesize);

    if (!updatetree) {
        ADIO_Offset start;
        /* use quicker version */
        typehelper_calcrange(data->view_etype,
                             data->view_ftype, data->view_disp, offset,
                             memtypesize * count, &start, &lastofs);

        if (data->filesize < lastofs)
            data->filesize = lastofs;

        if (data->hints.debug)
            debuginfo("trackwrite: lastofs=%lu, filesize=%lu\n", (long unsigned) lastofs,
                      (long unsigned) data->filesize);
    }
    else {
        DatatypeHandler cb;
        logfs_processtypes_info info;
        info.datalogstart = datalogstart;
        info.tree = &data->tree;
        cb.start = 0;
        cb.stop = 0;
        cb.startfragment = 0;
        cb.stopfragment = 0;
        cb.processdata = logfs_processtypes_addtree;

        typehelper_calcaccess(data->view_etype, data->view_ftype,
                              data->view_disp, offset, memtypesize * count, &cb, &info);

        /* Since we update the tree, we can find the latest byte touched
         * (in this operation or in an older one) in the span of the tree */
        rtree_range range;
        rtree_get_range(data->tree.rtree, &range);

        if (data->filesize < range.stop)
            data->filesize = range.stop;

        if (data->hints.debug)
            debuginfo("trackwrite: filesize: %lu treerange: %lu-%lu\n",
                      (long unsigned) data->filesize, (long unsigned) range.start,
                      (long unsigned) range.stop);
    }
}


/* offset is in etypes */
int logfs_writedata(ADIO_File fd, const void *buf,
                    int count, MPI_Datatype memtype, ADIO_Offset ofs, int collective)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);
    ADIO_Offset datalogpos;

    /*ADIOI_Flatlist_node * file; */
    /*int memtypecontig;  */
    int update_tree;
    int track_filesize;

    /* store write in logfile; doesn't use filetype and stores offset
     * relative to view displacement and etype */
    datalogpos = logfs_file_record_write(data->logfsfile, buf, count, memtype, ofs);

    /* if TRACK_FILESIZE isn't defined, only track filesize when
     * reading from the file is allowed (meaning readmode level higher than
     * LOGFS_READMODE_NONE);
     *
     * if TRACK_FILESIZE is on, always track the filesize */
#ifdef LOGFS_TRACK_FILESIZE
    track_filesize = 1;
#else
    track_filesize = (data->readmode > LOGFS_READMODE_NONE ? 1 : 0);
#endif

    /* We keep an up to date tree tracking writes only for
     * LOGFS_READMODE_FULL */
    update_tree = (data->readmode >= LOGFS_READMODE_FULL ? 1 : 0);

    /* if we did an untracked write, the rtree is worthless
     * Free the memory if we invalidate an existing one */
    if (!update_tree) {
        if (data->rtree_valid) {
            /* free memory if needed */
            rtree_clear(data->tree.rtree);
            data->tree.rangesize = 0;
            data->rtree_valid = 0;
        }
    }

    if (update_tree || track_filesize) {
        logfs_trackwrite(data, memtype, count, ofs, update_tree, track_filesize, datalogpos);
    }

    /* Non-zero writes invalidate the real file until the
     * next replay */
    if (count)
        data->file_valid = 0;

    return 1;
}

int logfs_flush(ADIO_File fd)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);

    /* flush logfiles */
    logfs_file_flush(data->logfsfile);

    /* flush underlying ringbuffers
     * (also flushes underlying files) */
    writering_flush(data->writedata);
    writering_flush(data->writemeta);

    return 1;
}



int logfs_resize(ADIO_File fd, MPI_Offset ofs)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);

    logfs_file_record_setsize(data->logfsfile, ofs);

    data->filesize = ofs;

    return 1;
}

int logfs_set_view(ADIO_File fd, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype filetype)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);

    /* record the old view as native since that is the only one supported */
    logfs_file_record_view(data->logfsfile, etype, filetype, disp, "native");

    if (data->view_etype != MPI_DATATYPE_NULL)
        MPI_Type_free(&data->view_etype);
    if (data->view_ftype != MPI_DATATYPE_NULL)
        MPI_Type_free(&data->view_ftype);

    MPI_Type_dup(etype, &data->view_etype);
    MPI_Type_dup(filetype, &data->view_ftype);
    data->view_disp = disp;

    /* cache some data here; We need it anyway
     * (unless we're in write-only mode) */
    MPI_Type_extent(data->view_ftype, &data->view_ftype_extent);
    MPI_Type_size(data->view_ftype, &data->view_ftype_size);
    MPI_Type_size(data->view_etype, &data->view_etype_size);


    return 1;
}


ADIO_Offset logfs_getfsize(ADIO_File fd)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);

#ifndef LOGFS_TRACK_FILESIZE
    /* if track filesize is not set, it is illegal to call getfsize in wr-only
     * mode */
    assert(data->readmode > LOGFS_FILE_READ_NONE);
#endif
    return data->filesize;
}


/*===========================================================================
//===== replay ==============================================================
//==========================================================================*/

typedef struct {
    logfs_file_typeinfo *ftype;
    logfs_file_typeinfo *etype;
    int ftype_size;
    int ftype_extent;
    int etype_extent;
    int etype_size;
    int ftype_cont;
    ADIO_Offset disp;
    ADIO_Offset size;
    int epoch;
    logfs_rtree *tree;
    ADIO_LOGFS_Data *data;
} logfs_replay_data;


static inline void logfs_replay_freetype(logfs_file_typeinfo * info)
{
    if (!info)
        return;
    ADIOI_Free(info->blocklens);
    ADIOI_Free(info->indices);
    ADIOI_Free(info);
}


static int logfs_replay_init(void *data)
{
    logfs_replay_data *rep = (logfs_replay_data *) data;

    if (rep->data->hints.debug) {
        debuginfo("logfs_replay_init ...\n");
    }

    rep->ftype_size = 1;
    rep->ftype_extent = 1;
    rep->etype_size = 1;
    rep->etype_extent = 1;
    rep->ftype = 0;
    rep->etype = 0;
    rep->ftype_cont = 1;
    rep->epoch = -1;
    rep->disp = 0;
    return 1;
}

static int logfs_replay_start_epoch(void *data, int epoch)
{
    logfs_replay_data *rep = (logfs_replay_data *) data;


    if (rep->data->hints.debug) {
        debuginfo("Start epoch: epoch num=%lu\n", (unsigned long) epoch);
    }

    rep->epoch = epoch;
    return 1;
}

static int logfs_replay_set_view(void *data, ADIO_Offset disp,
                                 logfs_file_typeinfo * etype, logfs_file_typeinfo * ftype,
                                 const char *datarep)
{
    logfs_replay_data *rep = (logfs_replay_data *) data;

    if (rep->data->hints.debug) {
        debuginfo("replay write: set view: disp=%lu datarep=%s\n", (unsigned long) disp, datarep);
    }


    logfs_replay_freetype(rep->etype);
    logfs_replay_freetype(rep->ftype);

    rep->etype = etype;
    rep->ftype = ftype;
    rep->disp = disp;

    rep->etype_extent = logfs_file_typeinfo_extent(etype);
    rep->ftype_extent = logfs_file_typeinfo_extent(ftype);
    rep->ftype_size = logfs_file_typeinfo_size(ftype);
    rep->etype_size = logfs_file_typeinfo_size(etype);
    rep->ftype_cont = logfs_file_typeinfo_continuous(ftype);

    return 1;
}

static int logfs_replay_set_size(void *data, ADIO_Offset size)
{
    logfs_replay_data *rep = (logfs_replay_data *) data;
    rep->size = size;
    return 1;
}


/* TODO: check this for logfs_rtree (rangesize?) */
/* size is in bytes, offset is location in datalog */
static int logfs_replay_write(void *data, ADIO_Offset writeofs, int size, ADIO_Offset datalogstart)
{
    logfs_replay_data *rep = (logfs_replay_data *) data;
    int ftypecount;
    int ftyperemainder;
    ADIO_Offset fileofs;
    int i;
    int j;
    ADIO_Offset ofs = writeofs;

    assert(rep->epoch >= 0);

    if (rep->data->hints.debug) {
        debuginfo("replay write: writeofs=%lu, size=%lu, \n");
    }

    /* we have view data (flatbuf,extent,size) in rep */

    /* calc byte start offset */
    ofs *= rep->etype_size;
    ofs += rep->disp;

    if (rep->ftype_cont) {
        /* continuous case */
        ADIO_Offset start = ofs;
        ADIO_Offset stop = ofs + size;
        logfs_rtree_addsplit(rep->tree, start, stop, datalogstart);
        return 1;
    }


    /* calculate amount in replay */
    ftypecount = size / rep->ftype_size;
    ftyperemainder = size % rep->ftype_size;


    /* we have a flatbuf rep */
   /* MPI-IO file views can be tiled */
   for (i=0; i<ftypecount; ++i)
   {
       /* examine each element of the flattened representation */
      for (j=0; j<rep->ftype->count; ++j)
      {
	  /* fileofs: offset in canonical file.  Computation was wrong for
	   * cases where datatype lower bound was non-zero:
	   * - ofs: user-provided offset of this request.  we got that value
	   * directly from the .meta log file
	   * - i*rep->ftype_extent: the idiomantic way to deal with tiled file
	   *   views
	   * - rep->ftype->indices[j] - rep->ftype->indices[0]:  When
	   * indices[0] is zero (when lower bound is zero), this does nothing.
	   * When lower bound is non-zero this adjusts the offsets relative to
	   * the lower bound.  However the offsets do not need adjusting! */

	 fileofs = ofs + i*rep->ftype_extent +
	    rep->ftype->indices[j];

	 logfs_rtree_addsplit(rep->tree,
		 fileofs, fileofs + rep->ftype->blocklens[j], datalogstart);

	 /* datalogstart: posistion in .data file */
            datalogstart += rep->ftype->blocklens[j];
        }
    }

    ofs += ftypecount * rep->ftype_extent;

    while (ftyperemainder) {
        ADIO_Offset increment;
        increment = (ftyperemainder > rep->ftype->blocklens[j] ?
                     ftyperemainder : rep->ftype->blocklens[j]);

        logfs_rtree_addsplit(rep->tree, ofs, ofs + increment, datalogstart);

        ftyperemainder -= increment;
        datalogstart += increment;
    }

    return 1;
}

static int logfs_replay_done(void *data)
{
    logfs_replay_data *rep = (logfs_replay_data *) data;

    /* free etype/ftype */
    logfs_replay_freetype(rep->etype);
    logfs_replay_freetype(rep->ftype);

    if (rep->data->hints.debug) {
        debuginfo("replay done\n");
        /* dump tree */
        rtree_dump(rep->tree->rtree);
    }
    return 1;
}


/* replay current epoch and build rtree rep (adding to possibly existing one) */
static void logfs_replay_buildrtree(ADIO_LOGFS_Data * data, int all)
{
    logfs_file_replayops ops;
    logfs_replay_data repdata;

    ops.init = logfs_replay_init;
    ops.start_epoch = logfs_replay_start_epoch;
    ops.set_view = logfs_replay_set_view;
    ops.set_size = logfs_replay_set_size;
    ops.write = logfs_replay_write;
    ops.done = logfs_replay_done;

    /* clear out old rtree if any */
   if (!(data->tree.rtree) ) return;
    rtree_clear(data->tree.rtree);
    data->tree.rangesize = 0;


    repdata.tree = &data->tree;
    repdata.data = data;

    logfs_file_replay(data->logfsfile, !all, &ops, &repdata);
}



int logfs_sync(ADIO_File fd)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);
    ADIO_Offset filesize;

    if (data->hints.debug)
        debuginfo("------------------- [LogFS] Sync called ---------------\n");

    logfs_file_record_sync(data->logfsfile);

    /* when LOGFS_TRACK_FILESIZE is defined, always keep an accurate
     * filesize after a sync, even when in write-only mode */
#ifndef LOGFS_TRACK_FILESIZE
    if (data->readmode > LOGFS_READ_NONE) {
#endif
        MPI_Allreduce(&data->filesize, &filesize, 1, ADIO_OFFSET, MPI_MAX, data->comm);
        data->filesize = filesize;
#ifndef LOGFS_TRACK_FILESIZE
    }
#endif

    /* since we did a sync (causing a replay in r/w mode) our tree
     * can be emptied and is valid */
    rtree_clear(data->tree.rtree);
    data->tree.rangesize = 0;
    data->rtree_valid = 1;

    /* sync the real file */
    if (MPI_FILE_NULL != data->realfile_single)
        MPI_File_sync(data->realfile_single);
    if (MPI_FILE_NULL != data->realfile_collective)
        MPI_File_sync(data->realfile_collective);
    return 1;
}


//===========================================================================
//===== Read support ========================================================
//===========================================================================

/*
 * TODO:
 *   possible optimizations:
 *      1) if the full rtree is available and up to date, use that one
 *         to read directly from the logfile what is available and
 *         read the rest from the real file
 *      2) If the full rtree is not available, do a replay of the epoch
 *         and record only regions of interest to our current read request
 *         (by first converting the datatype into an rtree (put
 *         ADIOI_INVALID_OFFSET everywhere) and then replaying and updating
 *         (modified rtree_add_split) ; then read again from real file and
 *         logfile
 */

/* For now, we just replay the last epoch (collective or not)
 * and then read from the real file
 */
/* note: offset is in bytes, not etypes */
int logfs_readdata(ADIO_File fd, void *buf,
                   int count, MPI_Datatype memtype, ADIO_Offset offset, int collective,
                   MPI_Status * status)
{
    ADIO_LOGFS_Data *data = logfs_data(fd);
    MPI_File *file;


   /* what does it mean to replay a log file when the file is opened read-only?
    * We will have to assume the user knows what he/she asked for (hah!), and
    * that something else replayed the file already (as in replay-on-close). */
   if (!data->file_valid && !(data->user_amode & MPI_MODE_RDONLY))
   {
        /* If the file is not valid, replay once;
         * Keep the file marked as valid until the first write operation */

        /* Full replay */
        logfs_replay_helper(data, collective);
        data->file_valid = 1;
    }

    /* now we can just read from the real file */

    /* set view */
   if (collective)
       file = &data->realfile_collective;
   else
       file = &data->realfile_single;

   MPI_File_set_view (*file, data->view_disp, MPI_BYTE,
	   data->view_ftype, "native", MPI_INFO_NULL);

   if (collective) {
    return MPI_File_read_at_all(*file, offset, buf, count, memtype, status);
   } else {
       return MPI_File_read_at(*file, offset, buf, count, memtype, status);
   }
}

/*
int logfs_readdata (ADIO_File fd, void * buf, int count,
      MPI_Datatype memtype, ADIO_Offset offset, int collective)
{
    if we have a valid rtree use it find out where to read data
    * if not:
    *   1) build the full tree and keep it around so that
    *      we can also handle a next read (but junk it again on the first
    *      write)
    *   2) build a partial tree only describing data dealing with the read
    *   request; this tree cannot be reused
    */

   /* use the tree to lookup every part of the mem read */
   /* generate a type for reading in the datafile,
    * also generate a type for reading in the real file*/

   /* start 2 nonblocking reads (since they could be on different filesystems
    * (e.g. local disk and netwerk filesys) we want to maximize bandwidth usage) */

   /* problem: in independent mode, cannot change the view on the real file!
    *

    assert (0);
    }
    */


/*
 * Replay the log, passing all data to the user;
 * Let him/her solve the consistency issue.
 */
static int logfs_user_replay(ADIO_LOGFS_Data * data)
{
    assert(data->user_replay_cb.init);
    assert(data->user_replay_cb.done);
    return 0;
}
