#include <malloc.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

#ifdef HAVE_EFENCE
#include <efence.h>
#endif

#include "writering.h"


#define LOCK_FREE       0
#define LOCK_WRITE_ACTIVE 1
#define LOCK_READ_ACTIVE 2

/**
 * Helper for writecombining and readahead 
 *
 */ 

typedef struct 
{
   char *           data;               /* data pointer */ 
   WRR_OFFSET       startofs;           /* Start offset in file         */ 
   unsigned int     dirty;              /* Block needs to be written    */ 
   unsigned int     locked;             /* write or read is in progress */  
   unsigned int     used;               /* How much data is in the block */ 
} writering_block; 

struct writering_instance
{
   writering_ops  ops;       /* external helper operations */
   void * ops_data;          /* extra data for external helper */

   unsigned int maxblockcount; /* max block count */ 
   unsigned int blocksize;     /* size in bytes of a block */
   unsigned int blockcount;    /* number of blocks in use */ 

   writering_block * blocks;  /* Pointer to array of pointers to blocks */ 

   int  write_active;       /* if a write is in progress */ 
   int  read_active;        /* if a read is in progress */ 
   unsigned int write_size; /* Size of non-blocking write */ 
   unsigned int read_size;  /* Size of active non-blocking read request */ 
   unsigned int write_ready; /* Number of full dirty blocks in queue */ 

   int  readmode;      /* If reading is allowed */ 
   int  writemode;     /* If writing is allowed */ 

   int sync;           /* to turn off all buffering */ 

   int open;           /* for lazy opening */

   unsigned int lastusedblock; /* last used block (reading/writing) */ 
   WRR_OFFSET   filesize;      /* Size of file (logical) */ 
   WRR_OFFSET   lastread;      /* Offset of last read operation */
   unsigned int readops;       /* Number of consecutive ordered read ops */ 

   int debug;          /* do extra checking */ 
}; 


typedef struct writering_instance writering_instance;


/* =========================================================================== */ 

/* forwards */ 
void writering_progress (writering_handle handle); 


/* ======================================================================= */

#define WRR_MIN(a,b) ( (a) < (b) ? (a) : (b) )
#define WRR_MAX(a,b) ( (a) > (b) ? (a) : (b) )

static inline void writering_assert (int t)
{
   if (t)
      return; 
   fprintf (stderr, "Writering: assertion failed!\n"); 
   *(char*) 0 = 10; 
}

/* ======================================================================= */

/*  
 * Make sure the helper is active so that we can read/write 
 */ 
inline static void writering_ensureopen (writering_handle handle)
{
   WRR_OFFSET realfilesize; 
   if (handle->open)
      return; 

   handle->ops.init (handle->ops_data, handle->readmode, handle->writemode); 
   handle->open = 1; 

   handle->ops.getsize (handle->ops_data, &realfilesize); 
   handle->filesize = WRR_MAX (handle->filesize, realfilesize); 
}


/* ======================================================================= */
/* ======================================================================= */
/* ======================================================================= */

static void writering_write_immediate (writering_handle handle, 
      WRR_OFFSET ofs, const void * data, unsigned int datasize)
{
   unsigned int written; 

   /* cannot do this if a write is already active */ 
   assert  (handle->write_active < 0); 

   /*fprintf (stderr, "immediate write @ %lu, %lu bytes\n", 
        (long unsigned) ofs, (long unsigned) datasize); */
   writering_ensureopen (handle); 
   handle->ops.start_write (handle->ops_data, ofs, 
         data, datasize); 
   handle->ops.wait_write (handle->ops_data, &written); 

   writering_assert (written == datasize); 
}

/* ======================================================================= */
/* ======================================================================= */
/* ======================================================================= */

/*
 * Read non-blocking
 */
static int writering_read_immediate (writering_handle handle,
      WRR_OFFSET ofs, void * data, unsigned int size)
{
   unsigned int read; 

   /* can't have any nonblocking read going on */ 
   assert (handle->read_active < 0); 


   writering_ensureopen (handle); 
   handle->ops.start_read (handle->ops_data, ofs, data, size); 
   handle->ops.wait_read (handle->ops_data, &read); 
   return read; 
}


static int writering_validate_overlap (const writering_block * b1, 
      const writering_block * b2)
{
   /* order first block in b1 */ 
   if (b1->startofs > b2->startofs)
   {
      const writering_block * tmp = b1; 
      b1 = b2; 
      b2 = tmp; 
   }

   /* b1 is first block */ 
   return (b2->startofs < (b1->startofs + b1->used)); 
}

/* validate internal data structure */ 
static void writering_validate (writering_consthandle handle)
{
   unsigned int i,j ; 
   assert (handle->blockcount <= handle->maxblockcount); 

   for (i=0; i<handle->blockcount; ++i)
   {
      const writering_block * ptr = &handle->blocks[i]; 

      if (!ptr->used)
         continue; 

      assert (ptr->data); 
      assert (ptr->used <= handle->blocksize); 

      assert (ptr->startofs + ptr->used <= handle->filesize); 

      /* check non-overlapping */ 
      for (j=i+1; j<handle->blockcount; ++j)
      {
         const writering_block * ptr2 = &handle->blocks[j]; 
         assert (!ptr2->used || 
               !writering_validate_overlap (ptr, ptr2)); 
      }
   }
}

/* ======================================================================= */
/* == Non-blocking reads/writes ========================================== */
/* ======================================================================= */

/* Find a block suitable for non-blocking write */
/* Return block number or -1 if none found */ 
static int writering_write_nonblock_select 
                (writering_consthandle handle)
{
   int minblock = -1;
   int i; 
   WRR_OFFSET minofs; 

   /* go over the block list, and only consider full dirty blocks;
    * Select the earliest in the fule among all full blocks, and return that one. */

   if (!handle->write_ready)
      return -1; 

   if (handle->write_active >= 0)
      return -1; 

   for (i=0; i<handle->blockcount; ++i)
   {
      if (!handle->blocks[i].used || !handle->blocks[i].dirty || 
            handle->blocks[i].locked != LOCK_FREE)
      {
         continue; 
      }

      if (minblock < 0 || (handle->blocks[i].startofs < minofs))
      {
         minblock = i; 
         minofs = handle->blocks[i].startofs; 
      }
   }
   assert (minblock == -1 || (minblock >= 0 && minblock < handle->blockcount)); 
   return minblock; 
}


/* return true if a non-blocking write is going on */ 
static inline int writering_write_nonblock_active 
        (writering_consthandle handle)
{
   return handle->write_active >= 0; 
}

/* return true if a non-blocking read is going on */ 
static inline int writering_read_nonblock_active 
        (writering_consthandle handle)
{
   return handle->read_active; 
}

/* Try to start nonblocking write */ 
static void writering_write_nonblock_start (writering_handle handle)
{
   int block = writering_write_nonblock_select (handle); 
   writering_block * blockptr; 

   if (block < 0)
      return; 

   blockptr = &handle->blocks[block]; 

   assert (handle->write_ready); 
   --handle->write_ready; 

   assert (blockptr->locked == LOCK_FREE); 
   blockptr->locked = LOCK_WRITE_ACTIVE; 
   
   writering_ensureopen (handle); 
   handle->ops.start_write (handle->ops_data, blockptr->startofs,
         blockptr->data, blockptr->used); 
   
   handle->write_active = block; 
   handle->write_size = blockptr->used; 
}

static int writering_read_nonblock_test (writering_handle handle)
{
   assert (handle->read_active >= 0); 
   assert (0); 
   return 0; 
}

static void writering_read_nonblock_finished (writering_handle handle, 
      unsigned int size)
{
   assert (handle->read_active >= 0); 
   assert (0); 
}

static void writering_write_nonblock_finished (writering_handle handle)
{
   writering_block * block; 
   assert (handle->write_active >= 0); 

   block = &handle->blocks[handle->write_active]; 

   block->dirty = 0; 
   block->locked = LOCK_FREE; 
   handle->write_active = -1; 
}

static int writering_write_nonblock_test (writering_handle handle)
{
   unsigned int written; 
   assert (handle->write_active >= 0); 

   if (handle->ops.test_write (handle->ops_data, &written))
   {
      // If this fails, there was an error writing the data 
      assert (written == handle->write_size); 
      writering_write_nonblock_finished (handle); 
      return 1; 
   }
   else
   {
      return 0; 
   }
}

static void writering_read_nonblock_wait (writering_handle handle)
{
   assert (handle->read_active >= 0); 
   unsigned int read; 
   handle->ops.wait_read (handle->ops_data, &read); 
   writering_read_nonblock_finished (handle, read); 
}

static void writering_write_nonblock_wait (writering_handle handle)
{
   assert (handle->write_active >= 0); 
   unsigned int written; 
   handle->ops.wait_write (handle->ops_data, &written); 
   assert (written == handle->write_size); 
   writering_write_nonblock_finished (handle); 

}


/* ======================================================================= */
/* ======================================================================= */
/* ======================================================================= */


/* 
 * Constructor
 */
writering_handle writering_create (int blocksize, int maxblockcount,
      const writering_ops * ops, void * opsdata, int read, int write)
{
   writering_instance * data = 
      (writering_instance *) malloc (sizeof(writering_instance)); 

   data->ops = *ops; 
   data->ops_data = opsdata; 

   data->maxblockcount = maxblockcount; 
   data->blocksize = blocksize; 

   data->blockcount = 0; 
   
   data->blocks = (writering_block*) malloc (sizeof(writering_block)*data->maxblockcount); 

   data->open = 0; 
   data->sync = 0; 

   /* reading/writing is allowed if we have 1 or more blocks allocated */ 
   data->writemode = write;
   data->readmode = read; 

   data->write_active = -1; 
   data->read_active = -1; 
   data->write_ready = 0; 

   data->filesize = 0; 
   data->lastread = 0; 
   data->readops = 0; 

   data->lastusedblock = 0; 
   data->debug = 0; 

   return data; 
}

/* ==========================================================================*/
/* ==========================================================================*/
/* ==========================================================================*/

void writering_setdebug (writering_handle handle, int debug)
{
   handle->debug = debug; 
}

void writering_setsync (writering_handle handle, int sync)
{
   /* If going to sync mode, flush writes/reads first */ 
   if (sync)
   {
      if (handle->writemode)
              writering_write_flush (handle); 
      if (handle->readmode)
              writering_read_flush (handle); 
      /* no point in keeping unused memory */
      writering_reducemem (handle); 
   }
   handle->sync = sync; 
}

/* ==========================================================================*/
/* ==========================================================================*/
/* ==========================================================================*/

void writering_free (writering_handle * handle)
{
   assert (handle); 
   assert (*handle); 

   /* There shouldn't be any more read/write operations,
    * so reducemem should be able to free all memory */ 
   writering_write_flush (*handle); 
   writering_reducemem (*handle); 
   assert (0 == (*handle)->blockcount); 
   free ((*handle)->blocks); 

   /* Close the helper */ 
   if ((*handle)->open)
   {
      (*handle)->open = 0; 
      (*handle)->ops.done ((*handle)->ops_data); 
   }

   free (*handle); 
   
   *handle = 0; 
}

/* ==========================================================================*/
/* ==========================================================================*/
/* ==========================================================================*/

/* Invalidate all cached reads */ 
void writering_read_flush (writering_handle handle)
{
   int i; 
   assert (handle); 

   /* Flush doesn't affect the logical write position, 
    * only the real fileposition */ 

   /* if there is nothing in memory we cannot flush anything */
   if (!handle->blockcount)
      return; 

   /* if there are no reads pending, skip waiting */
   if (handle->read_active < 0)
       return;

   /* Wait until pending read is done */ 
   writering_read_nonblock_wait (handle); 
    
   /* Clear all read buffers */ 
   for (i=0; i<handle->blockcount; ++i)
   {
      /* Clear all non-dirty blocks */ 
      if (handle->blocks[i].used && !handle->blocks[i].dirty)
         handle->blocks[i].used = 0; 
   }
}

/*=========================================================================*/
/*=========================================================================*/
/*=========================================================================*/

/* Flush all buffered writes; Data becomes available as read buffer */ 
void writering_write_flush (writering_handle handle)
{
   int i; 

   if (!handle->blockcount)
      return; 

   /* Wait for pending write to complete */ 
   if (handle->write_active >= 0)
      writering_write_nonblock_wait (handle); 

   for (i=0; i<handle->blockcount; ++i)
   {
      if (handle->blocks[i].used && handle->blocks[i].dirty)
      {
         /* Do nonblocking write */ 
         writering_write_immediate (handle, handle->blocks[i].startofs,
               handle->blocks[i].data, handle->blocks[i].used); 
         /* Clear dirty flag */ 
         handle->blocks[i].dirty = 0; 
      }
   }
}

/*=========================================================================*/
/*=========================================================================*/
/*=========================================================================*/

void writering_getsize (writering_handle handle, WRR_OFFSET * ofs)
{
   /* get filesize */ 
   writering_ensureopen (handle); 
   *ofs = handle->filesize; 
}

/*=========================================================================*/
/*=========================================================================*/
/*=========================================================================*/

void writering_progress (writering_handle handle)
{
   /* Test for completion of a pending read */ 
   writering_read_nonblock_test (handle); 

   /* test for completion of pending write;
    * If no more active writes, try to flush the first full dirty block */ 
   writering_write_nonblock_test (handle);
   if (!handle->write_active)
      writering_write_nonblock_start (handle); 
}

/*=========================================================================*/
/*=========================================================================*/
/*=========================================================================*/

static inline int writering_blockcontains (writering_handle handle, 
      unsigned int blocknum, WRR_OFFSET ofs)
{
   return (handle->blocks[blocknum].startofs <= ofs && 
         (handle->blocks[blocknum].startofs+handle->blocksize) > ofs); 
}

/* find block that contains this offset; -1 if not found;
   Blocks that have used == 0 are considered unallocated blocks */ 
static inline int writering_findblock (writering_handle handle, WRR_OFFSET ofs)
{
   int i; 

   if (handle->lastusedblock >= 0 
         && handle->lastusedblock < handle->blockcount)
   {
      /* check last block */ 
      if (writering_blockcontains (handle, handle->lastusedblock, ofs)
             && handle->blocks[handle->lastusedblock].used)
         return handle->lastusedblock; 
   }

   /* not cached, search blocks */ 
   for (i=0; i<handle->blockcount; ++i)
   {
      if (handle->blocks[i].used && writering_blockcontains (handle, i, ofs))
      {
         handle->lastusedblock = i; 
         return i; 
      }
   }
   return -1; 
}


/* First select try to select a block before lastread (preferably a non-dirty
 * non-locked one);
 * If this fails, select the last block in file order */
unsigned int writering_reclaimblock_readmode (writering_handle handle)
{
   unsigned int i; 
   int ret = -1; 
   unsigned int maxofs = 0; 
   WRR_OFFSET maxstartofs = handle->blocks[0].startofs; 
   int dirtyret = -1; 
   WRR_OFFSET startofs; 
   WRR_OFFSET dirtystartofs; 
   /* Try to reclaim a block before readofs */ 
   for (i=0; i<handle->blockcount; ++i)
   {
      writering_block * ptr = &handle->blocks[i]; 

      /* unused blocks should have been selected earlier on */ 
      assert (ptr->used); 

      /* find last block in file order */ 
      if (ptr->startofs > maxstartofs)
      {
         maxofs = i; 
         maxstartofs = ptr->startofs; 
      }

      /* Find first non-dirty block in file order before lastread */ 
      if ((ptr->startofs + ptr->used) < handle->lastread)
      {
         if (ret < 0 || ptr->startofs < startofs)
         {
            ret = i; 
            startofs = ptr->startofs; 
         }
         if (dirtyret < 0 || ptr->startofs < dirtystartofs)
         {
            dirtyret = i; 
            dirtystartofs = ptr->startofs; 
         }
      }

   }

   /* try non-dirty/locked block before lastread */ 
   if (ret >= 0)
      return ret; 

   /* try dirty/locked block before lastread */ 
   if (dirtyret >= 0)
      return dirtyret; 

   /* choose block with highest startofs */ 
   return maxofs; 
}


/*
 * In writemode we are only worried about not blocking the writer;
 * For now, just reuse any non-dirty buffer (preferring the smallest one); 
 * If there are none, write the buffer with the most data.
 */
unsigned int writering_reclaimblock_writemode (writering_handle handle)
{
   int clean = -1; 
   int full = -1; 
   unsigned int fullsize; 
   unsigned int cleansize; 
   unsigned int i; 

   for (i=0; i<handle->blockcount; ++i)
   {
      writering_block * ptr = &handle->blocks[i]; 

      /* Empty block :-) */ 
      if (!ptr->used)
        return i; 

      if (!ptr->dirty)
      {
         if (clean < 0 || ptr->used < cleansize)
         {
            clean = i; 
            cleansize = ptr->used; 
         }
      }
      else
      {
         if (full < 0 || ptr->used > fullsize)
         {
            full = i; 
            fullsize = ptr->used; 
         }
      }
   }
   if (clean >= 0)
      return clean;
   assert (full >= 0); 
   return full; 
}


/*
 * Return a free block;
 * Either reclaim a used block, or (preferably) create a new one.
 *
 * We use readops as an indication of what phase we're in;
 * If readops > 0 we're most likely sequentially reading through the file.
 */
static unsigned int writering_reclaimblock (writering_handle handle)
{
   int ret = -1;  
   writering_block * ptr; 
   int i; 

   /* Try to add a new block */ 
   if (handle->blockcount < handle->maxblockcount)
   {
      unsigned int ret = handle->blockcount++; 
      writering_block * ptr = &handle->blocks[ret]; 
      ptr->used = 0; 
      ptr->data = (char*) malloc (handle->blocksize); 
      ptr->locked = LOCK_FREE; 
      ptr->dirty = 0; 
      ptr->startofs = (WRR_OFFSET) -1; 
      assert (ptr->data); 
      return ret; 
   }

   /* Try to find an unused block */ 
   for (i=0; i<handle->blockcount; ++i)
   {
      if (!handle->blocks[i].used)
      {
         ret = i; 
         break; 
      }
   }

   if (ret < 0)
   {
      if (handle->readops)
         ret= writering_reclaimblock_readmode (handle); 
      else
         ret= writering_reclaimblock_writemode (handle); 
   }

   /* we have decided on a block, clean it if needed and return */ 

   ptr = &handle->blocks[ret]; 

   /* If it was used, we might need to clean up */ 
   if (ptr->used)
   {
      switch (ptr->locked)
      {
         case LOCK_WRITE_ACTIVE:
            writering_write_nonblock_wait (handle); 
            break; 
         case LOCK_READ_ACTIVE:
            writering_read_nonblock_wait (handle); 
            break; 
         case LOCK_FREE:
            break; 
         default:
            assert (0); 
      }

      assert (ptr->locked == LOCK_FREE); 

      if (ptr->dirty)
      {
         if (handle->write_active >= 0)
            writering_write_nonblock_wait (handle); 
         assert (handle->write_active < 0); 
         writering_write_immediate (handle, 
               ptr->startofs, ptr->data, ptr->used); 
         ptr->dirty = 0; 
      }

      ptr->used=0; 
   } 

   return ret; 
}

/* 
 * Remove all blocks that could contain data from this range (even if they
 * don't have any data in that range now);
 */ 
static void writering_clear (writering_handle handle, WRR_OFFSET ofs, 
      unsigned int size)
{
   int i; 
   WRR_OFFSET range_start, range_stop; 

   range_start = ofs; range_stop = range_start + size;

   for (i=0; i<handle->blockcount; ++i)
   {
      writering_block * ptr; 
      if (!handle->blocks[i].used)
         continue; 

      WRR_OFFSET block_start = handle->blocks[i].startofs;
      WRR_OFFSET block_stop = block_start + handle->blocksize; 

      if (block_stop <= range_start || block_start >= range_stop)
         continue; 

      /* block is in range */ 
      assert ( (block_start >= range_start && block_stop <= range_stop) 
            || (block_start < range_start && block_stop <= range_stop) 
            || (block_start > range_start && block_stop >= range_stop));

      ptr = &handle->blocks[i]; 
      if (ptr->locked)
      {
         if (ptr->locked == LOCK_WRITE_ACTIVE)
         {
            writering_write_nonblock_wait (handle); 
         }
         else
         {
            assert (ptr->locked == LOCK_READ_ACTIVE); 
            writering_read_nonblock_wait (handle); 
         }
         assert (ptr->locked == LOCK_FREE);             
      }

      /* if the block is dirty, write it to disk */ 
      if (ptr->dirty)
      {
         if (handle->write_active >= 0)
            writering_write_nonblock_wait (handle); 
         assert (handle->write_active < 0); 
         writering_write_immediate (handle, ptr->startofs,
               ptr->data, ptr->used); 
      }

      /* mark block as unused */ 
      ptr->used = 0; 
      ptr->locked = 0; 
      ptr->startofs = (WRR_OFFSET) -1; 
     
   }

#ifndef NDEBUG
   if (handle->debug)
      writering_validate (handle); 
#endif
}

void writering_write (writering_handle handle, WRR_OFFSET ofs, 
      const void * data, unsigned int size)
{
   int block; 
   writering_block * blockptr; 
   unsigned int todo = size; 
   WRR_OFFSET curofs = ofs; 
   
   handle->readops = 0; 
   handle->lastread = 0; 

   if (!size)
      return; 

   if (handle->sync)
   {
      /* track filesize */ 
      if (ofs + size > handle->filesize)
         handle->filesize = ofs + size; 
      writering_write_immediate (handle, ofs, data, size); 
      return; 
   }

   /* nonblocking mode */ 
   
   while (todo)
   {
      unsigned int thiswrite; 
      /* See if we have a memory block for this write */
      block = writering_findblock (handle, curofs); 

      if (block>= 0)
      {
         blockptr = &handle->blocks[block]; 

         /* check that we're not creating a hole in the block */ 
         if ((blockptr->startofs + blockptr->used) < curofs)
         {
           unsigned int readsize; 
           unsigned int read; 

            /* wait until background read finishes */ 
            if (handle->read_active >= 0)
               writering_read_nonblock_wait (handle);
            assert (handle->read_active < 0); 

            /* try to read from ptr->used until end of block */ 
            /* NOTE: we could be trying to read past the end of the file,
             * and we can only know for sure if the file is already open;
             * However, we do know that no block has an overlapping range so
             * we don't have to consider reading from in-memory blocks */ 

            /* ensureopen also updates the file size 
             * (in case we're writing to an existing file that extends beyond
             * the furthest byte written to the writering) */ 
            writering_ensureopen (handle); 

            assert (blockptr->startofs < handle->filesize); 

            readsize = WRR_MIN(handle->blocksize - blockptr->used, 
                        handle->filesize - blockptr->startofs - blockptr->used);

            /* when the buffered (logical) filesize is larger than the flushed 
             * filesize we could be reading past the end of the file here */ 
            read = 
               writering_read_immediate (handle, blockptr->startofs + blockptr->used, 
                               (char*) blockptr->data + blockptr->used, readsize); 

            if (read < readsize)
            {
               fprintf (stderr, "writering: warning: reading uninitialised data\n"); 
               memset ((char*) blockptr->data + blockptr->used + read, 
                     0, readsize - read); 
            }

            blockptr->used += readsize; 
         }
      }
      else
      {
         /* we need to come up with a new block */ 
         block = writering_reclaimblock (handle); 
         blockptr = &handle->blocks[block]; 
         blockptr->used = 0; 
         blockptr->startofs = curofs; 

         /* Need to clear blocks in our range to avoid overlapping blocks */ 
         writering_clear (handle, blockptr->startofs, handle->blocksize); 
      }

      /* add data to the block */ 
      thiswrite =  WRR_MIN(handle->blocksize - (curofs - blockptr->startofs),
                        todo);

      memcpy ((char *) (blockptr->data) + (curofs - blockptr->startofs), 
            data, thiswrite); 

      /* update dirty & used */ 
      blockptr->dirty = 1; 
      blockptr->used = WRR_MAX(blockptr->used, curofs - blockptr->startofs + thiswrite); 
      data = (char*) data + thiswrite; 
      todo -= thiswrite; 
      curofs += thiswrite; 

      /* track filesize */ 
      if (curofs > handle->filesize)
         handle->filesize = curofs; 
   }

#ifndef NDEBUG
   if (handle->debug)
      writering_validate (handle); 
#endif
}


int writering_read (writering_handle handle, WRR_OFFSET ofs,
       void * data, unsigned int size)
{
   const unsigned int requested = size; 

   if (ofs > handle->lastread || !handle->readops)
      ++handle->readops;
   handle->lastread = ofs; 

   if (!size)
      return 0; 

   if (handle->sync)
      return writering_read_immediate (handle, ofs, data, size); 


   /* check if we have the block; if not; wait for any background reads and do
    * an immediate read; 
    * */ 


   while (size)
   {
      if (ofs >= handle->filesize)
         /* reading beyond end of file */ 
         break; 

      int blocknum = writering_findblock (handle, ofs); 
      writering_block * ptr; 
      unsigned int thisread; 

      if (blocknum < 0)
      {
         /* We don't have the block */ 
         /* Try to reclaim a block and read the whole block from the file */ 
         blocknum = writering_reclaimblock (handle); 
         unsigned int read; 

         writering_ensureopen (handle); 
       
         /* find blocks still overlapping with our range and free them */ 
         ptr = &handle->blocks[blocknum]; 
         ptr->startofs = ofs; 
         ptr->used = 0; 
         writering_clear (handle, ptr->startofs, handle->blocksize); 


         /* read data into the block */ 
         ptr->used = WRR_MIN (handle->blocksize, handle->filesize - ofs); 
         read = writering_read_immediate (handle, ptr->startofs, 
               ptr->data, ptr->used); 

         if (read < ptr->used)
         {
            /* fill with zeros; 
             * This is needed when the logical filesize is larger than ofs
             * (because we did a write past ofs) but the real file hasn't been
             * updated yet. Reading from the file will result in less than
             * ptr->used bytes in this case */ 
            memset ((char*) ptr->data + read, 0, ptr->used - read); 
         }
      }

      /* read data from blocknum */ 
      ptr = &handle->blocks[blocknum]; 

      /* writering_findblock return the block that *could* hold the data;
       * It doesn't mean it has the data; So we check for the case where 
       * the data we want to read isn't in the block */ 
      if (ptr->startofs + ptr->used <= ofs)
      {
         unsigned int read; 

         /* We're not reading beyond the filesize, since we checked for that;
          * We read the rest from the block (up to EOF) from the file 
          * (since there could be old data in there if the file existed before
          * we started writing); If the read failed we fill with zero's */ 
         unsigned int readsize;

         /* ensureopen could modify handle->filesize */ 
         writering_ensureopen (handle); 
         
         readsize = WRR_MIN(
                         handle->filesize - ptr->startofs - ptr->used, 
                         handle->blocksize - ptr->used); 

         if (handle->read_active >= 0)
            writering_read_nonblock_wait (handle); 
         
         read = writering_read_immediate (handle,  ptr->startofs + ptr->used, 
               (char*) ptr->data + ptr->used, readsize);
         assert (read >= 0); 
         if (read != readsize)
         {
            assert (read < readsize); 
            fprintf (stderr, "warning: writering: reading uninitialized data!\n"); 
            memset ((char*) ptr->data + ptr->used + read, 0, 
                  readsize - read); 
         }
         ptr->used += readsize;
         thisread = WRR_MIN(size, ptr->used - (ofs - ptr->startofs)); 
      }
      else
      {
              assert (ptr->startofs <= ofs); 
              thisread = WRR_MIN (size, ptr->used - (ofs - ptr->startofs));  
      }

      memcpy (data, (char*) ptr->data + (ofs - ptr->startofs), thisread); 
      ofs += thisread; 
      size -= thisread; 
      data = (char*) data + thisread; 
   }

#ifndef NDEBUG
   if (handle->debug)
      writering_validate (handle); 
#endif

   return requested - size; 
}

/*=========================================================================*/
/*=========================================================================*/
/*=========================================================================*/

void writering_reducemem (writering_handle handle)
{
   unsigned int i, front, back; 
   unsigned int check = 0; 

   if (!handle->blockcount)
      return; 

   /* pass 1: free all memory that can be released */ 
   for (i=0; i<handle->blockcount; ++i)
   {
      if (handle->blocks[i].used && handle->blocks[i].dirty)
      {
         ++check; 
         continue;
      }

      handle->blocks[i].used = 0; 
      handle->blocks[i].dirty = 0; 
      free (handle->blocks[i].data); 
      handle->blocks[i].data = 0; 
   }

   /* Pass 2: compress list */ 
   front = 0; back = handle->blockcount-1; 
      
   while (1)
   {
      if (front == handle->blockcount)
      {
         /* Nothing could be released; return */ 
         return; 
      }

      if (!handle->blocks[front].data)
      {
         /* Current block is empty */ 
         /* Find nonempty block at the back */ 
         while (!handle->blocks[back].data && back > front)
            --back;
         if (back == front)
         {
            /* no more full blocks...*/
            break;
         }
         /* Switch full and empty block */ 
         handle->blocks[front] = handle->blocks[back]; 
         handle->blocks[back].data = 0; 
      }

      ++front; 
   }; 

   /* double check, since I was half asleep when writing this*/ 
/*   assert (handle->blockcount == front); */
   for (i=0; i<front; ++i)
      assert (handle->blocks[i].data); 
   for (i=front; i<handle->blockcount; ++i)
      assert (!handle->blocks[i].data); 

   /* Front points to the first empty block */ 
   handle->blockcount = front; 
}


void writering_flush (writering_handle handle)
{
   writering_write_flush (handle); 
   writering_read_flush (handle); 
}


void writering_reset (writering_handle handle, WRR_OFFSET size)
{
   unsigned int i; 
   /* file might not yet be open, but that's strange...  */
   if (! handle->open) return;

   for (i=0; i<handle->blockcount; ++i)
   {
      writering_block * ptr = &handle->blocks[i]; 
      if (!ptr->used)
         continue; 

      if (ptr->startofs + ptr->used < size)
         continue; 

      /* block is completely in the discarded part; 
       * discard block */ 
      if (ptr->startofs >= size)
      {
         ptr->startofs = (WRR_OFFSET) -1; 
         ptr->used = 0; 
         continue; 
      }

      /* block has to be discarded partially; check for pending writes */
      /* if block is active wait */ 
      if (handle->read_active == i)
         writering_read_nonblock_wait (handle); 
      if (handle->write_active == i)
         writering_write_nonblock_wait (handle); 

      assert (ptr->startofs < size); 
      ptr->used = ptr->startofs + ptr->used - size; 
   }

   /* all blocks after size are non-active and discarded/truncated 
    * set filesize */ 
   handle->ops.reset (handle->ops_data, size); 
   handle->filesize = size; 
}
