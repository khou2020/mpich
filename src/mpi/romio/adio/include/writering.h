/****************************************************************************
 *   Write optimization for LogFS
 *   (Could also be used when for SEQUENTIAL opened files in MPI-IO)
 *
 *    keep a number of large memory buffers; accept all writes and 
 *    append them to the active buffer; If a buffer is full, start a
 *    nonblocking write for it.
 *
 *    When running out of buffers, wait until the nonblocking until a buffer
 *    gets written to disc so that it can be reused
 *
 *    IDEA: to avoid copying, writes larger than a block could 
 *       1) cause a seek and be written in a nonblocking way
 *       2) cause a flush (to avoid the seek) and be written nonblocking
 *
 *    For now, they are added to the cache as is everything else
 *
 *    There will only be ONE nonblocking write at the same time
 *
 */

#ifndef ROMIO_LOGFS_WRITERING_H
#define ROMIO_LOGFS_WRITERING_H


#include "writering_types.h"


/* To make it easy to test inside and outside ROMIO 
 * Inside ROMIO, WRR_OFFSET is defined to be ADIO_OFFSET 
 */
#ifndef WRR_OFFSET
#error WRR offset type is not defined!
#endif

struct writering_instance;

typedef struct writering_instance *        writering_handle; 
typedef const struct writering_instance *  writering_consthandle; 


typedef struct
{
   /* will be called when te writering is created 
    *   read / write indicate which operations are needed
    */
   int (*init) (void * opsdata, int read, int write); 

   /* will be called when te writering is destroyed */
   int (*done) (void * opsdata); 

   /* initiate a write operation */
   int (*start_write) (void * opsdata, WRR_OFFSET ofs, const void * data, unsigned int size);
   
   /* return true if the write is finished */
   int (*test_write) (void * opsdata, unsigned int *written); 

   /* wait until the write is finished */
   int (*wait_write) (void * opsdata, unsigned int *written);

   /* flush if supported; Will not be called when a 
    * write/read is in progress */
   int (*flush) (void * opsdata); 

   /* truncate file to given size */
   int (*reset) (void * opsdata, WRR_OFFSET ofs); 

   /* return filesize; will only be called when file is open */
   int (*getsize) (void * opsdata, WRR_OFFSET * ofs); 

   /* start read at specified byte ofs */
   int (*start_read) (void * opsdata, WRR_OFFSET ofs, void * data, unsigned int size); 

   /* test for read finish; set bytes read */
   int (*test_read) (void * opsdata, unsigned int * read); 

   /* wait for read finish; set bytes read */
   int (*wait_read) (void * opsdata, unsigned int * read); 

} writering_ops; 


/* readblockcount == 0: no reading allowed; >0: reads allowed
 * writeblockcount == 0: no writing allowed; >0: writes allowed
 */
writering_handle writering_create (int blocksize, int maxblockcount,
      const writering_ops * operations, void * data, 
      int read, int write);

void writering_free (writering_handle * handle); 


/* try to free mem by releasing all non-dirty blocks 
 * (this is all readahead data + all flushed write data)*/
void writering_reducemem (writering_handle handle); 

/* progress writing if needed */
void writering_progress (writering_handle handle);

/* if sync is true, disable all read-ahead/write-behind */ 
void writering_setsync (writering_handle handle, int sync); 

/* read from file; return bytes read, -1 if error, 0 indicates EOF*/
int writering_read (writering_handle handle, WRR_OFFSET ofs, void * buf, 
        unsigned int size); 

void writering_write (writering_handle handle, WRR_OFFSET ofs, 
        const void * data, unsigned int size); 

/* FLush data in write cache */ 
void writering_write_flush (writering_handle handle); 

/* Flush data in read cache */ 
void writering_read_flush (writering_handle handle); 

/* do read & write flush */ 
void writering_flush (writering_handle handle);


/* truncate file to given position; resets read and write pointer */
void writering_truncate (writering_handle handle, WRR_OFFSET ofs); 

/* return filesize */
void writering_getsize (writering_handle handle, WRR_OFFSET * ofs);


/* set debug mode */ 
void writering_setdebug (writering_handle handle, int debug); 

void writering_reset (writering_handle handle, WRR_OFFSET size); 

#endif
