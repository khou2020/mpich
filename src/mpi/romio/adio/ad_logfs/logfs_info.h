#ifndef ROMIO_LOGFS_INFO_H
#define ROMIO_LGOFS_INFO_H

/*
 * Header for that groups the info keys that have an effect on a logfs 
 * instance 
 */

/* readmode: one of track_none, track_phased, track_all  */ 
#define LOGFS_INFO_READMODE        "logfs_readmode"

/* debug logfs */ 
#define LOGFS_INFO_DEBUG           "logfs_debug"

/* Time replays */ 
#define LOGFS_INFO_TIMEREPLAY      "logfs_timereplay"

/* NOTE: xxxBLOCKCOUNT and xxxBLOCKSIZE should both be set,
 * otherwise they have no effect */ 

/* number of blocks in write buffer */
#define LOGFS_INFO_DATABLOCKCOUNT   "logfs_datablockcount"

/* size of a block in the write buffer */
#define LOGFS_INFO_DATABLOCKSIZE    "logfs_datablocksize" 

/* number of blocks in metadata buffer */
#define LOGFS_INFO_METABLOCKCOUNT   "logfs_metablockcount"

/* size of a block in the metadata buffer */
#define LOGFS_INFO_METABLOCKSIZE    "logfs_metablocksize" 

/* size of intermediate buffer for replaying log files */
#define LOGFS_INFO_FLUSHBLOCKSIZE "logfs_flushblocksize"

/* set base dir for log files */
#define LOGFS_INFO_LOGBASE          "logfs_info_logbase"

/* for sync mode (no lazy writing) 
 * In sync mode, no write buffering is done 
 * (ignoring the blockcount/blocksize keys )*/
#define LOGFS_INFO_SYNC         "logfs_sync" 

/* replay the file when closing */ 
#define LOGFS_INFO_REPLAYCLOSE     "logfs_replayonclose"

#endif
