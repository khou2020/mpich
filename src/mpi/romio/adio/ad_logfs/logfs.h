/****************************************************
 *   High level logfs support functions             *
 ****************************************************/

#ifndef ROMIO_LOGFS_H
#define ROMIO_LOGFS_H

#include "adio.h"
#include "layered.h"

/* do we need to track file size even in wronly mode? */

/* Currently, the filesize is only consistent AFTER a sync point is reached
 * (open, close, sync)
 *
 * see: MPI standard:
 *   "When applying consistency semantics, calls to MPI_FILE_SET_SIZE and
 *   MPI_FILE_PREALLOCATE are considered writes to the file (which conflict
 *   with operations that access bytes at displacements between the old and
 *   new file sizes), and MPI_FILE_GET_SIZE is considered a read of the file
 *   (which overlaps with all accesses to the file)."
 */
#define LOGFS_TRACK_FILESIZE

/* Supported read levels
 *
 *      NONE  :  no reading allowed
 *      SOME  :  lots of writes, mixed with a rare read
 *      PHASED:  reading and writing phases distinct;
 *               In this mode, the tree is constructed at the first read
 *               and is discarded at the next write
 *      FULL  :  Full tracking; For mixed read and write workloads
 */
typedef enum {
    LOGFS_READMODE_NONE = 0,
    LOGFS_READMODE_SOME,
    LOGFS_READMODE_PHASED,
    LOGFS_READMODE_FULL
} logfs_readmode_kind;

struct ADIO_LOGFS_Data;

/* return true if we are in logfs: prefix mode */
static inline int logfs_standalone(ADIO_File fd)
{
    return (fd->file_system == ADIO_LOGFS);
}

static inline struct ADIO_LOGFS_Data *logfs_data(ADIO_File fd)
{
    if (logfs_standalone(fd))
        return (struct ADIO_LOGFS_Data *)fd->fs_ptr;
    return (struct ADIO_LOGFS_Data *)ADIOI_Layer_get_data(fd);
}

/* check if logfs is active on the filehandle */
int logfs_active(ADIO_File fd);

/* force replay (sync) of fd */
int logfs_replay(ADIO_File fd, int collective);

/* collective; logfs init */
int logfs_activate(ADIO_File fd, MPI_Info info);

/* collective; logfs deactivate */
int logfs_deactivate(ADIO_File fd);

/* deletes log files associated with the given file */
int logfs_delete(const char *filename);

/* return true if the given filename has a logfs log attached */
int logfs_probe(MPI_Comm comm, const char *filename);

/* store update:
 * uses current view and displacement
 * Only requires the offset to be in etypes relative to the displacement
 */
int logfs_writedata(ADIO_File fd, const void *buf,
                    int count, MPI_Datatype memtype, ADIO_Offset offset, int collective);

int logfs_readdata(ADIO_File fd, void *buf,
                   int count, MPI_Datatype memtype, ADIO_Offset offset,
                   int collective, MPI_Status *status);

/* flush  logfiles and real file */
int logfs_flush(ADIO_File fd);

/* resize */
int logfs_resize(ADIO_File fd, MPI_Offset newsize);

/* set view */
int logfs_set_view(ADIO_File fd, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype ftype);

/* file sync op called */
int logfs_sync(ADIO_File fd);

/* Adjust hints */
void logfs_setinfo(ADIO_File fd, MPI_Info info);

/* copy hints */
void logfs_transfer_hints(MPI_Info source, MPI_Info dest);

/* return current logical file size */
ADIO_Offset logfs_getfsize(ADIO_File fd);

#endif
