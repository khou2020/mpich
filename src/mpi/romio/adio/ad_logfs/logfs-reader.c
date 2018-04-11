/* here's a simple utility to read the logfs .logfs and .meta files.  These
 * structures are all internal to logfs and defined in ad_logfs/logfs.c, so if
 * they are modified there, one will have to manually update the structures
 * here.
 *
 * if while reading the file you get an assertion (see the 'UNKNOWN RECORD TYPE
 * IN LOG ' comment below), that might be because the log file was generatd
 * with (or without) magic guards */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>

#include <stdlib.h>

#include <stdio.h>

#include <assert.h>
typedef struct
{
    char magic[64]; /* file magic */
    int flags;
    int logfilecount;      /* max. number of lock files possibly created for
                            this file (acros reopens) (== number of CPUs
                            used in open/create) */
    int epoch;             /* next epoch number (used in reopen) */
    char logfilebase[255]; /* base filename for logfiles */
} logfs_logfsfile_header;

/* from logfs_file.c */
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
//#define LOGFS_FILE_RECORDMAGIC

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

char *logfs_flags_to_string(int flags)
{
    if (flags == 2)
        return "REPLAY";
    if (flags == 3)
        return "ACTIVE";
    return NULL;
}
void dump_logfs(char *filename)
{
    int fd, ret;
    logfs_logfsfile_header h;

    fd = open(filename, O_RDONLY);
    ret = read(fd, &h, sizeof(h));
    printf("magic: %s flags %s count %d epoch %d base |%s|\n",
           h.magic, logfs_flags_to_string(h.flags),
           h.logfilecount, h.epoch, h.logfilebase);
}

void extract_typemap(char *prefix, int fd, FILE *output)
{
    int64_t count;
    int64_t *indices;
    int64_t *blocklens;
    int ret, i;
    ret = read(fd, &count, sizeof(count));
    indices = malloc(count * sizeof(*indices));
    blocklens = malloc(count * sizeof(*blocklens));
    ret = read(fd, indices, count * sizeof(*indices));
    ret = read(fd, blocklens, count * sizeof(*blocklens));
    fprintf(output, "%s ", prefix);
    for (i = 0; i < count; i++)
    {
        fprintf(output, "(%ld %ld) ", indices[i], blocklens[i]);
    }
    free(indices);
    free(blocklens);
}

void dump_logfs_view(int fd_in, FILE *output)
{
    int ret;
    int64_t displacement;
    ret = read(fd_in, &displacement, sizeof(displacement));
    fprintf(output, "\ndisplacement: %ld ", displacement);
    extract_typemap("etype:", fd_in, output);
    extract_typemap("ftype:", fd_in, output);
}

void dump_logfs_meta_data(int fd_in, FILE *output)
{
    int ret, size;
    int64_t fileofs, datalogofs;
    ret = read(fd_in, &size, sizeof(size));
    ret = read(fd_in, &fileofs, sizeof(fileofs));
    ret = read(fd_in, &datalogofs, sizeof(datalogofs));
    fprintf(output, "\nsize: %d fileofs: %ld datalogofs: %ld\n",
            size, fileofs, datalogofs);
}

void dump_logfs_meta(char *filename, FILE *output)
{
    int fd, ret, epoch;
    logfs_file_recordstruct record;
    logfs_file_headerstruct h;
    fd = open(filename, O_RDONLY);
    /* read header, but mostly to just skip over it */
    ret = read(fd, &h, sizeof(h));

    while (1)
    {
        ret = read(fd, &record, sizeof(record));
        if (ret <= 0)
            break;

        switch (record.recordtype)
        {
        case LOGFS_FILE_RECORD_VIEW:
            dump_logfs_view(fd, output);
            break;

        case LOGFS_FILE_RECORD_DATA:
            dump_logfs_meta_data(fd, output);
            break;
        case LOGFS_FILE_RECORD_SYNC:
            ret = read(fd, &epoch, sizeof(epoch));
            fprintf(output, "\nsync-epoch: %d ", epoch);
            break;

        default:
            printf("found record type %d\n", record.recordtype);
            assert(0 /* UNKNOWN RECORD TYPE IN LOG */);
        }
    }
}
void dump_logfs_data(char *filename)
{
}

int main(int argc, char **argv)
{
    int c;
    while ((c = getopt(argc, argv, "l:m:d:")) != -1)
    {
        switch (c)
        {
        case 'l':
            dump_logfs(optarg);
            break;
        case 'm':
            dump_logfs_meta(optarg, stdout);
            break;
        case 'd':
            dump_logfs_data(optarg);
            break;
        default:
            printf("unknown argument\n");
        }
    }
}
