#include <assert.h>
#include "adio.h"
#include "adio_extern.h"
#include "logfs_file.h"
#include "typehelper.h"

/* ======================================================================= */
/* ============= LOGFS_FILE RECORDTYPE =================================== */
/* ======================================================================= */

/*
 * The displacement and datatype follow the record header
 */
#define LOGFS_FILE_RECORD_VIEW 1

/*
 * After this header kind:
 *   - seek position (in etypes)
 *   - datasize (in bytes)
 *
 *   The datasize (in bytes) followed by the
 * actual data
 */
#define LOGFS_FILE_RECORD_DATA 2

/*
 * Followed by the epoch number
 */
#define LOGFS_FILE_RECORD_SYNC 3

/*
 * followed by filesize (MPI_Offset)
 */
#define LOGFS_FILE_RECORD_SETSIZE 4

/* used for debugging; If enabled, magicstart and magicstop will
 * be written before and after the recordheader */
/* #define LOGFS_FILE_RECORDMAGIC */

#define LOGFS_FILE_RECORDMAGIC_START "[magicstart]    "
#define LOGFS_FILE_RECORDMAGIC_STOP "[magicstop ]    "

/* Record struct for in the logfs metalog */
typedef struct
{
#ifdef LOGFS_FILE_RECORDMAGIC
    char magic_start[16];
#endif
    int recordtype;
    double timestamp;
#ifdef LOGFS_FILE_RECORDMAGIC
    char magic_stop[16];
#endif
} logfs_file_recordstruct;

/* Header that goes at the beginning of both metadata and datafiles */
typedef struct
{
    char magic[64];
    /* don't store epoch here; otherwise we need an alreduce when we lazily
     * open the logfile to inform other CPUs of the highest epoch number */
    /*int epoch;  */ /* epoch number of last epoch in file */
} logfs_file_headerstruct;

/*
 * Handles all logfs meta- and datafile I/O
 */
struct logfs_file_instance
{
    MPI_Comm comm; /* file communicator */

    /* external functions for writing to a logfile */
    logfs_file_ops ops;
    void *ops_data;

    /* external functions for reading from a logfile */
    logfs_file_readops readops;
    void *readops_data;

    ADIO_Offset datalog_offset;  /* offset of next write in datalog */
    ADIO_Offset metalog_offset;  /* offset of next write in metalog */
    ADIO_Offset dataepoch_start; /* start offset of epoch in datalog */
    ADIO_Offset metaepoch_start; /* start offset of epoch in metalog */
    int last_epoch;

    int active;     /* has there been real data yet */
    int readactive; /* if the file is open for reading */

    /* do we need to record updates to view/size/epoch before the next data
     * write? */
    int dirty_view;
    int dirty_size;
    int dirty_sync;

    /* file state info */
    int epoch;
    ADIO_Offset filesize;
    ADIO_Offset displacement;
    MPI_Datatype etype;
    MPI_Datatype filetype;

    /* used for converting memdata into a cont. buffer
     * When switching to dataloop: think of doing partial writes (starting
     * write as soon as 1 block of data is available) & writing
     * directly into writering mem*/
    DatatypeHandler memdecodeops;

    /* used for replay: current read position in logfile */
    ADIO_Offset readpos;
};

/***************************************************************************/
/*** forwards **************************************************************
 ***************************************************************************/

static int logfs_file_acceptmemdata(void *membuf, int size, ADIO_Offset fileoffset, void *data);

static void logfs_file_writeheader(logfs_file_handle handle);

static inline int logfs_file_read(logfs_file_handle handle, void *data, int size);

static void logfs_file_flush_size(logfs_file_handle handle);

static inline void logfs_file_readdatatype(logfs_file_handle handle, logfs_file_typeinfo *info);

static inline void logfs_file_readseek(logfs_file_handle handle, ADIO_Offset pos);

static int logfs_file_readheader(logfs_file_handle handle, logfs_file_headerstruct *h);

/***************************************************************************/

logfs_file_handle logfs_file_create(MPI_Comm comm, const logfs_file_ops *ops, void *ops_data,
                                    const logfs_file_readops *readops, void *readops_data)
{
    logfs_file_handle handle = (logfs_file_handle)
        malloc(sizeof(struct logfs_file_instance));
    assert(handle);

    MPI_Comm_dup(comm, &handle->comm);

    handle->ops = *ops;
    handle->ops_data = ops_data;
    handle->readops = *readops;
    handle->readops_data = readops_data;

    handle->active = 0;
    handle->epoch = 0;
    handle->datalog_offset = 0;
    handle->metalog_offset = 0;

    handle->dirty_view = 0;
    handle->dirty_size = 0;
    handle->dirty_sync = 0;

    /* we rely on a set view after the create */
    handle->displacement = 0;
    handle->filesize = 0;
    handle->etype = MPI_DATATYPE_NULL;
    handle->filetype = MPI_DATATYPE_NULL;

    /* callback for streaming memory datatype contents */
    handle->memdecodeops.start = 0;
    handle->memdecodeops.startfragment = 0;
    handle->memdecodeops.stop = 0;
    handle->memdecodeops.stopfragment = 0;
    handle->memdecodeops.processdata = logfs_file_acceptmemdata;

    /* reading */
    handle->readactive = 0;
    handle->readpos = 0;

    return handle;
}

void logfs_file_free(logfs_file_handle *handle)
{
    /* flush setsize if needed */
    if ((*handle)->dirty_size)
        logfs_file_flush_size(*handle);

    if ((*handle)->readactive)
    {
        (*handle)->readops.done((*handle)->readops_data);
    }

    if ((*handle)->active)
    {
        (*handle)->ops.done((*handle)->ops_data);
    }

    if ((*handle)->etype != MPI_DATATYPE_NULL)
        MPI_Type_free(&(*handle)->etype);

    if ((*handle)->filetype != MPI_DATATYPE_NULL)
        MPI_Type_free(&(*handle)->filetype);

    MPI_Comm_free(&(*handle)->comm);

    free(*handle);
    *handle = 0;
}

static inline void logfs_file_openlogs(logfs_file_handle handle)
{
    /*int headerok; */
    logfs_file_headerstruct h;

    if (handle->active)
        return;

    handle->active = 1;

    handle->ops.init(handle->ops_data);

    /* try to see if there is already a header */

    /* try to read header */
    logfs_file_readseek(handle, 0);
    if (logfs_file_readheader(handle, &h))
    {
        /* read header was ok; move to end of logfile and metafile */
        handle->ops.getsize(handle->ops_data, &handle->datalog_offset, LOGFS_FILE_LOG_DATA);
        handle->ops.getsize(handle->ops_data, &handle->metalog_offset, LOGFS_FILE_LOG_META);
    }
    else
    {
        /* couldn't read header: trunc file */
        /* reset filesize and write new header */
        handle->ops.restart(handle->ops_data, 0, LOGFS_FILE_LOG_META);
        handle->metalog_offset = 0;
        handle->ops.restart(handle->ops_data, 0, LOGFS_FILE_LOG_DATA);
        handle->datalog_offset = 0;
        logfs_file_writeheader(handle);
    }
}

static inline void logfs_file_write(logfs_file_handle handle, const void *data, int size, int log)
{
    if (!handle->active)
        logfs_file_openlogs(handle);

    handle->ops.write(handle->ops_data,
                      (log ==
                               LOGFS_FILE_LOG_DATA
                           ? handle->datalog_offset
                           : handle->metalog_offset),
                      data,
                      size, log);

    /* keep track of offset in data log file */
    if (log == LOGFS_FILE_LOG_DATA)
        handle->datalog_offset += size;
    else
        handle->metalog_offset += size;
}

/* read flatlist view of datatype */
static inline void logfs_file_readdatatype(logfs_file_handle handle, logfs_file_typeinfo *info)
{
    logfs_file_read(handle, &info->count, sizeof(info->count));

    info->indices = ADIOI_Malloc(sizeof(*(info->indices)) * info->count);
    info->blocklens = ADIOI_Malloc(sizeof(*(info->blocklens)) * info->count);

    logfs_file_read(handle, info->indices, sizeof(*(info->indices)) * info->count);
    logfs_file_read(handle, info->blocklens, sizeof(*(info->blocklens)) * info->count);
}

static void logfs_file_writedatatype(logfs_file_handle handle, MPI_Datatype type)
{
    ADIOI_Flatlist_node *flat_buf;

    /* flattened code will store flattened representation as an attribute on
    * both built-in contiguous types and user-derrived types */
    flat_buf = ADIOI_Flatten_and_find(type);

    assert(flat_buf);

    /* write flattened version for now */
    /* write count, indices, blocklens */
    logfs_file_write(handle, &flat_buf->count, sizeof(flat_buf->count), LOGFS_FILE_LOG_META);
    logfs_file_write(handle, &flat_buf->indices[0],
                     sizeof(flat_buf->indices[0]) * flat_buf->count, LOGFS_FILE_LOG_META);
    logfs_file_write(handle, &flat_buf->blocklens[0],
                     sizeof(flat_buf->blocklens[0]) * flat_buf->count, LOGFS_FILE_LOG_META);
}

static int logfs_file_readheader(logfs_file_handle handle, logfs_file_headerstruct *h)
{
    return (logfs_file_read(handle, h, sizeof(logfs_file_headerstruct)) == sizeof(logfs_file_headerstruct));
}

static void logfs_file_writeheader(logfs_file_handle handle)
{
    logfs_file_headerstruct h;
    memset(h.magic, 0, sizeof(h.magic));
    strcpy(h.magic, "logfs\n");

    /* write a header to both files */
    logfs_file_write(handle, &h, sizeof(h), LOGFS_FILE_LOG_META);
    logfs_file_write(handle, &h, sizeof(h), LOGFS_FILE_LOG_DATA);
}

static inline void logfs_file_writerecordheader(logfs_file_handle handle, int recordtype)
{
    logfs_file_recordstruct s;
    s.recordtype = recordtype;
    s.timestamp = MPI_Wtime();
#ifdef LOGFS_FILE_RECORDMAGIC
    strncpy(s.magic_start, LOGFS_FILE_RECORDMAGIC_START, sizeof(s.magic_start));
    strncpy(s.magic_stop, LOGFS_FILE_RECORDMAGIC_STOP, sizeof(s.magic_stop));
#endif
    logfs_file_write(handle, &s, sizeof(s), LOGFS_FILE_LOG_META);
}

static void logfs_file_flush_size(logfs_file_handle handle)
{
    logfs_file_writerecordheader(handle, LOGFS_FILE_RECORD_SETSIZE);
    logfs_file_write(handle, &handle->filesize, sizeof(handle->filesize), LOGFS_FILE_LOG_META);
    handle->dirty_size = 0;
}

static void logfs_file_flush_sync(logfs_file_handle handle)
{
    /* record start of new epoch in metalog & datalog */
    handle->dataepoch_start = handle->datalog_offset;
    handle->metaepoch_start = handle->metalog_offset;
    handle->last_epoch = handle->epoch;

    logfs_file_writerecordheader(handle, LOGFS_FILE_RECORD_SYNC);
    logfs_file_write(handle, &handle->epoch, sizeof(handle->epoch), LOGFS_FILE_LOG_META);
    handle->dirty_sync = 0;
}

static void logfs_file_flush_view(logfs_file_handle handle)
{
    logfs_file_writerecordheader(handle, LOGFS_FILE_RECORD_VIEW);
    logfs_file_write(handle, &handle->displacement,
                     sizeof(handle->displacement), LOGFS_FILE_LOG_META);
    logfs_file_writedatatype(handle, handle->etype);
    logfs_file_writedatatype(handle, handle->filetype);
    handle->dirty_view = 0;
}

void logfs_file_record_sync(logfs_file_handle handle)
{
    /*ADIO_Offset filesize; */

    /* see if any of the others wrote a view change; if so, we need to do to
     * (because of collective nature of the calls); same for set_size
     *
     * So, allgather the dirty bits for epoch, view and setsize;
     *   1) they are all 0: -> no size/view changes since last sync -> do
     *   nothing
     *   2) All 1: view/setsize called, but nobody wrote a record -> do nothing
     *   3) Some are 1: there was a change, and some CPUs wrote a record
     *   -> Everybody who still has a 1 needs to flush the record
     */
    /* lazy writing of the epoch */

    /*int flags[3] = {handle->dirty_view ? 1 : 0,
     * handle->dirty_size ? 1 : 0,
     * handle->dirty_sync ? 1 : 0};
     * int recvbuf[3] = {0, 0, 0}; */

    /* since the dirty bits will be collective set to 1 (because of the fact
     * that the calls to set_size, sync, set_view are collective) finding a min
     * of 0 when we have a 1 means somebody did write a record so we have to do
     * so too */
    /*MPI_Allreduce (&flags[0], &recvbuf[0], 3, MPI_INT, MPI_MIN,  handle->comm);
     */
    /* so we have a zero and min is zero: everybody has zero; nothing happened
     * since last sync, so no need to flush */
    /* if we have more than the min, we need to flush (means we have dirty bit
     * and at least one of the others doens't) */

    /* problem here: need to have the order right otherwise there could be a
     * deadlock in the replay
     * Also: dirty bit needs to be extended into count because of the
     * following:
     *    set_size                             set_size
     *    write data (causes set_size flush)   do nothing
     *    set size                             set size
     *
     *    sync                                 sync
     *
     *    The sync would only cause one set size record to be written
     *    solution: probably need same detection logic in any of the collective
     *    calls (size,view,sync) which would solve the above situation by
     *    causing a flush on cpu B before the second set size
     *
     *    Problem: makes these ops collective again... consider not doing
     *    delayed set_size/set_view/sync at all which would avoid the allreduce
     *    on set_view,sync and setsize */

    /*if (flags[0] < recvbuf[0])
     * logfs_file_flush_view (handle);
     * if (flags[1] < recvbuf[1])
     * logfs_file_flush_size(handle);
     * if (flags[2] < recvbuf[2])
     * logfs_file_flush_sync (handle);
     */

    ++handle->epoch;
    handle->dirty_sync = 1;
}

void logfs_file_record_setsize(logfs_file_handle handle, MPI_Offset size)
{
    handle->filesize = size;
    if (!size)
    {
        /* TODO do something special and erase the log */
        /* or even better: handle this on a higher level;
         * this has nothing to do with the logfs_file as such*/
        handle->epoch = 0;
    }

    handle->dirty_size = 1;
}

void logfs_file_record_view(logfs_file_handle handle, MPI_Datatype etype,
                            MPI_Datatype filetype, MPI_Offset displacement, const char *datarep)
{
    if (handle->etype != MPI_DATATYPE_NULL)
        MPI_Type_free(&handle->etype);
    if (handle->filetype != MPI_DATATYPE_NULL)
        MPI_Type_free(&handle->filetype);

    MPI_Type_dup(filetype, &handle->filetype);
    MPI_Type_dup(etype, &handle->etype);

    handle->displacement = displacement;

    /* lazy writing of file view */
    handle->dirty_view = 1;
}

MPI_Offset logfs_file_record_write(logfs_file_handle handle,
                                   const void *buf, int count,
                                   MPI_Datatype memtype, MPI_Offset offset)
{
    int size;
    ADIO_Offset dataoffset;

    /* this always forces a state flush  */
    if (handle->dirty_view)
        logfs_file_flush_view(handle);
    if (handle->dirty_size)
        logfs_file_flush_size(handle);
    if (handle->dirty_sync)
        logfs_file_flush_sync(handle);

    logfs_file_writerecordheader(handle, LOGFS_FILE_RECORD_DATA);

    /* record number of bytes we are going to write */
    MPI_Type_size(memtype, &size);
    size *= count;
    logfs_file_write(handle, &size, sizeof(size), LOGFS_FILE_LOG_META);

    /* store the write offset in the file */
    logfs_file_write(handle, &offset, sizeof(offset), LOGFS_FILE_LOG_META);

    /* store the offset where our data will end up in the data log file */
    dataoffset = handle->datalog_offset;
    logfs_file_write(handle, &handle->datalog_offset,
                     sizeof(handle->datalog_offset), LOGFS_FILE_LOG_META);

    /* setup dataloop copy */
    typehelper_decodememtype(buf, count, memtype, &handle->memdecodeops, handle);

    /* check that the datafile is as we think it is */
    assert((dataoffset + size) == handle->datalog_offset);

    return dataoffset;
}

/* function gets called from within typehelper to copy the memory datatype
 * contents; Just accepts data and copies it into the logfile;
 * the type was already stored anyway*/
int logfs_file_acceptmemdata(void *membuf, int size, ADIO_Offset fileoffset, void *data)
{
    logfs_file_handle handle = (logfs_file_handle)data;

    logfs_file_write(handle, membuf, size, LOGFS_FILE_LOG_DATA);

    return 1;
}

void logfs_file_flush(logfs_file_handle handle)
{
    /* nothing to do; we cannot flush the logfiles, that is left up to whoever
     * provided us with the callbacks */
}

void logfs_file_clear(logfs_file_handle handle, int last)
{
    /* if nothing is written yet, don't start now */
    if (!handle->active)
        return;

    if (last)
    {
        /* if the last recorded epoch change is not equal to the current epoch,
         * there hasn't been a meaningful write operation in the current epoch
         * (otherwise the new epoch would have been recorded in the log)
         * As such, we don't have to do anything to clear the current epoch. */
        if (handle->epoch != handle->last_epoch)
            return;

        /* reset to last epoch */
        handle->ops.restart(handle->ops_data, handle->metaepoch_start, LOGFS_FILE_LOG_META);
        handle->metalog_offset = handle->metaepoch_start;

        handle->ops.restart(handle->ops_data, handle->dataepoch_start, LOGFS_FILE_LOG_DATA);
        handle->datalog_offset = handle->dataepoch_start;

        /* What else to do here? mark state as dirty? */
        return;
    }

    /* whole file needs to be cleared
     *  -> we can reset the epoch counter; rewrite the header
     */
    handle->epoch = 0;

    /* force rewrite of file header on next data write */
    /* we rewrite the header ourselves */
    /*handle->active = 0; */

    /* force rewrite of state on next data write */
    handle->dirty_view = 1;
    handle->dirty_size = 1;
    handle->dirty_sync = 1;

    handle->ops.restart(handle->ops_data, 0, LOGFS_FILE_LOG_META);
    handle->metalog_offset = 0;

    handle->ops.restart(handle->ops_data, 0, LOGFS_FILE_LOG_DATA);
    handle->datalog_offset = 0;

    /* write new header */
    logfs_file_writeheader(handle);
}

/* ======================================================================== */
/* === Read functions (for internal use) ================================== */
/* ======================================================================== */
static inline void logfs_file_readseek(logfs_file_handle handle, ADIO_Offset pos)
{
    assert(pos <= handle->metalog_offset);
    handle->readpos = pos;
}

/* do read operation at current position, increase read pos */
static inline int logfs_file_read(logfs_file_handle handle, void *data, int size)
{
    int read;
    if (!handle->readactive)
    {
        handle->readactive = 1;
        handle->readops.init(handle->readops_data);
    }

    read = handle->readops.read(handle->readops_data, handle->readpos,
                                data, size, LOGFS_FILE_LOG_META);
    handle->readpos += read;
    return read;
}

/* try to read a record; return 1 if OK, 0 if EOF */
static inline int logfs_file_readrecord(logfs_file_handle handle, logfs_file_recordstruct *header)
{
    int read = logfs_file_read(handle, header, sizeof(logfs_file_recordstruct));

#ifdef LOGFS_FILE_RECORDMAGIC
    /* If we could read something, it should be a valid header */
    if (read == sizeof(logfs_file_recordstruct))
    {
        assert(!strncmp(header->magic_start, LOGFS_FILE_RECORDMAGIC_START,
                        sizeof(header->magic_start)));
        assert(!strncmp(header->magic_stop, LOGFS_FILE_RECORDMAGIC_STOP,
                        sizeof(header->magic_stop)));
    }
#endif

    /* either EOF or we read a full record; other case means error! */
    assert(!read || sizeof(logfs_file_recordstruct) == read);

    return read;
}

/* ======================================================================== */
/* ======================================================================== */
/* ======================================================================== */
static inline int logfs_file_replay_processview(logfs_file_handle handle,
                                                const logfs_file_replayops *ops, void *opsdata)
{
    ADIO_Offset displacement;

    logfs_file_typeinfo *etype = (logfs_file_typeinfo *)
        ADIOI_Malloc(sizeof(logfs_file_typeinfo));
    logfs_file_typeinfo *ftype = (logfs_file_typeinfo *)
        ADIOI_Malloc(sizeof(logfs_file_typeinfo));

    logfs_file_read(handle, &displacement, sizeof(displacement));
    logfs_file_readdatatype(handle, etype);
    logfs_file_readdatatype(handle, ftype);

    return ops->set_view(opsdata, displacement, etype, ftype, "native");
}

static inline int logfs_file_replay_processdata(logfs_file_handle handle,
                                                const logfs_file_replayops *ops, void *opsdata)
{
    int size;
    ADIO_Offset datalogofs, fileofs;
    logfs_file_read(handle, &size, sizeof(size));
    logfs_file_read(handle, &fileofs, sizeof(fileofs));
    logfs_file_read(handle, &datalogofs, sizeof(datalogofs));

    return ops->write(opsdata, fileofs, size, datalogofs);
}

static inline int logfs_file_replay_processsync(logfs_file_handle handle,
                                                const logfs_file_replayops *ops, void *opsdata)
{
    int epoch;

    logfs_file_read(handle, &epoch, sizeof(epoch));
    return ops->start_epoch(opsdata, epoch);
}

static inline int logfs_file_replay_processsize(logfs_file_handle handle,
                                                const logfs_file_replayops *ops, void *opsdata)
{
    ADIO_Offset ofs;
    logfs_file_read(handle, &ofs, sizeof(ofs));
    return ops->set_size(opsdata, ofs);
}

int logfs_file_replay(logfs_file_handle handle, int last,
                      const logfs_file_replayops *ops, void *opsdata)
{
    /* offset in metalog where we start replaying */
    int active = 0; /* did we find something meaningful? */
    int cont = 1;
    logfs_file_recordstruct record;

    /* if nothing is written yet, don't start now */
    /* unless we reopenened a file! */
    if (!active && last)
        return 1;

    if (last)
    {
        /* if the last recorded epoch change is not equal to the current epoch,
         * there hasn't been a meaningful write operation in the current epoch
         * (otherwise the new epoch would have been recorded in the log)
         * As such, we don't have to do anything to clear the current epoch. */
        if (handle->epoch != handle->last_epoch)
            return 1;

        logfs_file_readseek(handle, handle->metaepoch_start);
    }
    else
    {
        logfs_file_headerstruct h;
        logfs_file_readseek(handle, 0);
        /* read header; increments replaypos */
        logfs_file_readheader(handle, &h);
    }

    /* loop over records
     *  -> read record; find out type; read rest; call callback */

    while (cont)
    {
        /* read record; if EOF break */
        int ok = logfs_file_readrecord(handle, &record);

        if (!ok)
            break;

        /* we found something, so make sure the callbacks are ready */
        if (!active)
        {
            active = 1;
            ops->init(opsdata);
        }

        /* process record; read aditional data */
        switch (record.recordtype)
        {
        case LOGFS_FILE_RECORD_VIEW:
            cont = logfs_file_replay_processview(handle, ops, opsdata);
            break;
        case LOGFS_FILE_RECORD_DATA:
            cont = logfs_file_replay_processdata(handle, ops, opsdata);
            break;
        case LOGFS_FILE_RECORD_SYNC:
            cont = logfs_file_replay_processsync(handle, ops, opsdata);
            break;
        case LOGFS_FILE_RECORD_SETSIZE:
            cont = logfs_file_replay_processsize(handle, ops, opsdata);
            break;
        default:
            assert(0 /* UNKNOWN RECORD TYPE IN LOG */);
            fprintf(stderr, "skipping unknown record type in log\n");
        };

        if (!cont)
            break;
    }

    if (active && ops->done)
        ops->done(opsdata);

    return cont;
}

void logfs_file_setepoch(logfs_file_handle handle, int epoch)
{
    int ep = epoch;
    MPI_Bcast(&ep, 1, MPI_INT, 0, handle->comm);
    assert(ep == epoch);

    handle->epoch = epoch;
    handle->dirty_sync = 1;
}
