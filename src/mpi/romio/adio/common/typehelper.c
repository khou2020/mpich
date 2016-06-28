#include <assert.h>
#include <stdio.h>
#include "typehelper.h"
#include "adio.h"
#include "adio_extern.h"


/*
 * Calculate every continuous segment in the access 
 */
void typehelper_calcaccess (MPI_Datatype etype, 
      MPI_Datatype ftype, ADIO_Offset disp, ADIO_Offset offset, 
      int writesize, const DatatypeHandler * cb, void * userdata)
{
   int ftypecontig; 
   int ftype_size;
   MPI_Aint ftype_extent;
   int etype_size; 
   int ftypecount;  /* number of complete ftypes in data */
   ADIOI_Flatlist_node * flat_buf; 
   int remainder; /* amount of bytes in last filetype */
   int i; 
   int j; 
   ADIO_Offset fileofs; 
   int status; 


   /* ftype_size and ftype_extent, etype_size are
    * calculated when setting the view */ 
   MPI_Type_size (etype, &etype_size); 

   offset *= etype_size; 
   offset += disp; 

   ADIOI_Datatype_iscontig (ftype, &ftypecontig); 
      
   if (cb->start)
	 cb->start (userdata); 
   
   if (ftypecontig)
   {
      /* easy case */

      if (cb->startfragment)
	 cb->startfragment (offset, writesize, userdata); 

      status = cb->processdata (0, writesize, offset, userdata); 

      if (cb->stopfragment)
	 cb->stopfragment (status, userdata); 
      if (cb->stop)
	 cb->stop (status, userdata); 
      return ; 
   }

   MPI_Type_extent (ftype, &ftype_extent); 
   MPI_Type_size (ftype, &ftype_size); 

   /* number of complete filetypes to write */ 
   ftypecount = (writesize) / ftype_size;
   /* number of bytes of the partial filetype */ 
   remainder = (writesize) % ftype_size; 

   flat_buf = ADIOI_Flatten_and_find(ftype);

   assert (flat_buf); 


   /* increase offset by leading hole in first filetype */
   if (ftypecount)
      offset += flat_buf->indices[0]; 


   /* we have ftype count complete filetypes */
   for (i=0; i<ftypecount; ++i)
   {
      for (j=0; j<flat_buf->count; ++j)
      {
	 fileofs = offset + flat_buf->indices[j] - flat_buf->indices[0] 
	    + (ftype_extent * i); 
	 if (cb->startfragment)
	    cb->startfragment (fileofs, flat_buf->blocklens[j], userdata); 

	 status = cb->processdata (0, flat_buf->blocklens[j], fileofs, userdata); 

	 if (cb->stopfragment)
	    cb->stopfragment (status, userdata); 

	 if (!status)
	    break; 
      }

      if (!status)
	 break; 

   }
     
   /* add extent for count filetypes */
   offset += ftypecount * ftype_extent; 

   /* do last filetype */ 
   i = 0; 
   while (remainder)
   {
      ADIO_Offset increment ; 

      assert (i < flat_buf->count); 
      /* is the final byte in this segment? */

      increment = (remainder < flat_buf->blocklens[i] ?
	    remainder : flat_buf->blocklens[i]); 

      /* add displacement of segment, relative to the beginning of the type  */
      offset += flat_buf->indices[i] - flat_buf->indices[0]; 
       
      if (cb->startfragment)
	 cb->startfragment (offset, increment, userdata); 

      status = cb->processdata (0, increment, offset, userdata); 

      if (cb->stopfragment)
	 cb->stopfragment (status, userdata); 

      ++i; 
      assert (remainder >= increment); 
      remainder -= increment; 
      if (!status)
	 break; 
   }

   if (cb->stop)
      cb->stop (status, userdata); 
}

/*
 * Given the parameters of a file view (the teype, ftype, displacement), an
 * offset, and an amount of data to write, determine the first (start) and last
 * (stop) bytes touched by the request.
 * this is looking at the file type (see etype, ftype and other file view
 * parameters), so take into account tiling, too.
 * Does not work as expected for LB/UB modified types;
 * The code will take the LB/UB as a 0 byte write and take it into account
 * when determining the first and last write position 
 */
void typehelper_calcrange (MPI_Datatype etype, 
      MPI_Datatype ftype, ADIO_Offset disp, ADIO_Offset offset, 
      int writesize, 
      ADIO_Offset * start, ADIO_Offset * stop)
{
   int ftype_size; 
   MPI_Aint ftype_lb, ftype_extent; 
   int etype_size; 
   int ftypecount;  /* number of complete ftypes in data */
   ADIOI_Flatlist_node * flat_buf; 
   int remainder; /* amount of bytes in last filetype */
   int i; 
   ADIO_Offset last_byte; /* last byte of type, ignoring UB marker */

   /* ftype_size and ftype_extent, etype_size are
    * calculated when setting the view */ 
   MPI_Type_get_extent (ftype, &ftype_lb, &ftype_extent);
   MPI_Type_size (ftype, &ftype_size); 
   MPI_Type_size (etype, &etype_size); 

   /* offset is a count of etype */
   offset *= etype_size; 
   /* but file view displacement is absolute bytes */
   offset += disp; 


   /* instead of special-casing contiguous types, they should follow the
    * noncontiguous code path */
   /* noncontiguous types a little trickier than contig.  Take into account
    * tiling of the type (count of type times extent), but also take into
    * account the UB markers.  Additional complication: the set_view code
    * already skips over any initial lower bound marker, so 'offset' could come
    * into this routine with a non-zero value */

   /* number of complete filetypes to write */ 
   ftypecount = (writesize) / ftype_size;
   /* number of bytes of the partial filetype */ 
   remainder = (writesize) % ftype_size; 

   flat_buf = ADIOI_Flatten_and_find(ftype);

   assert (flat_buf); 

   if (flat_buf->blocklens[flat_buf->count-1] == 0)
       /* the '-2' seems odd at first glance, but if there is an upper bound
	* marker then there must be at least two items in the flattened
	* representation */
       last_byte = flat_buf->indices[flat_buf->count -2] +
	   flat_buf->blocklens[flat_buf->count -2];
   else
       last_byte = flat_buf->indices[flat_buf->count -1] +
	   flat_buf->blocklens[flat_buf->count -1];

   /* offset now indicates first byte to write */
   *start = offset;

   /* before tiling these filetypes, need to (maybe) wind back the offset to
    * account for the lower bound (which set_file_view skipped over) */
   if (flat_buf->blocklens[0] == 0)
       offset -= flat_buf->indices[1];

     /* add extent for count filetypes.  these are the complete file types.
      * We'll do any partial file types below */
   offset += ftypecount * ftype_extent;

   if (remainder == 0) {
       /* no partial types to worry about, but need to trim off the upper
	* bound (if exists), since no type will tile after this one */
       offset -= (ftype_extent - ftype_lb) - last_byte;
   } else {
       for (i=0; remainder > 0; i++) {
	   remainder -= flat_buf->blocklens[i];
	   offset += flat_buf->indices[i] + flat_buf->blocklens[i];
       }
   }
   *stop = offset;
}

static int typehelper_decodememtype_contiguous 
  (void * buf, int count, MPI_Datatype memtype, 
   const DatatypeHandler * callback, void * data)
{
   int size; 
   int status; 
   MPI_Type_size (memtype, &size); 

   /* the easy case */
   if (callback->start)
      callback->start (data); 

   if (callback->startfragment)
      callback->startfragment (0, size, data); 

   status = callback->processdata (buf, size*count, 
	 0, data); 

   if (callback->stopfragment)
      callback->stopfragment (status, data); 

   if (callback->stop)
      callback->stop (status, data);

   return status ; 
}

/* stream the datatype contents (meant for memory datatypes) */
int typehelper_decodememtype (const void * buf, int count, 
      MPI_Datatype memtype,
      const DatatypeHandler * callback, void * data)
{
   ADIOI_Flatlist_node * flat_buf; 
   int continuous; 
   int status; 
   int i; 
   int j; 
   MPI_Aint extent; 
   char * ptr; 
   MPI_Offset bytecount ; 

   /* check for easy case */
   ADIOI_Datatype_iscontig (memtype, &continuous); 

   if (continuous)
      return typehelper_decodememtype_contiguous ((char *)buf, count, 
	    memtype, callback, data); 
   
   /* for now use the flatlist; later on use the dataloop code */
   MPI_Type_extent (memtype, &extent);

   /* find flattened version */
   flat_buf = ADIOI_Flatten_and_find(memtype);

   if (callback->start)
      callback->start (data); 

   ptr = (char *) buf; 
   bytecount = 0; 
   for (i=0; i<count; ++i)
   {
      for (j=0; j<flat_buf->count; ++j)
      {
	 ptr = ((char*)buf) + flat_buf->indices[j]
	    + i * extent; 
	 if (callback->startfragment)
	    callback->startfragment (bytecount, 
		  flat_buf->blocklens[j], data); 

	 status = callback->processdata (ptr, flat_buf->blocklens[j], 
	       bytecount, data); 

	 bytecount += flat_buf->blocklens[j]; 

	 if (callback->stopfragment)
	    callback->stopfragment (status, data); 
      }
   }

   if (callback->stop)
      callback->stop (status, data);

   return status; 
}


static inline MPI_Offset min_offset (MPI_Offset a, MPI_Offset b)
{
   return (a < b ? a : b); 
}

/* offset is in bytes, displacement is in bytes */ 
int typehelper_processtypes (MPI_Datatype memtype, void * buf, int count, 
      MPI_Datatype filetype, MPI_Datatype etype, int offset, int displacement, 
      const DatatypeHandler * callback, void * data)
{
   int memtypecontig; 
   int filetypecontig; 
   int transfersize; 
   int status = 1; /* return code */ 

   /* check for null operation */
   MPI_Type_size (memtype, &transfersize); 
   transfersize *= count; 
   if (0 == transfersize)
      return 0; 


   ADIOI_Datatype_iscontig (memtype, &memtypecontig); 
   ADIOI_Datatype_iscontig (filetype, &filetypecontig); 
      
   if (callback->start) 
	 callback->start (data); 

   if (memtypecontig && filetypecontig)
   {
      int size; 

      /* calculate byte size of the operation */
      MPI_Type_size (memtype, &size); 
      size *= count; 

      if (callback->startfragment) 
	 callback->startfragment (offset + displacement, size, data); 

      status = callback->processdata (buf, size, offset + displacement, data); 
      
      if (callback->stopfragment)
	 callback->stopfragment (status, data); 
   }
   else
   {
      MPI_Aint filetype_extent;
      int filetype_size; 
      int etype_size; 
      char * dataptr; 
      int i; 
      MPI_Offset writestart = offset + displacement; 
      int todo = transfersize; /* amount of data in bytes */

      ADIOI_Flatlist_node *flat_file;

      /* no noncontig memtypes for now */
      assert (memtypecontig); 

      /* contiguous in mem, noncont in file */
      /* flatten filetype */

      MPI_Type_extent (filetype, &filetype_extent); 
      MPI_Type_size (filetype, &filetype_size); 
      MPI_Type_size (etype, &etype_size); 

      flat_file = ADIOI_Flatten_and_find(filetype);
      assert (flat_file);
  
      /* now we have flat_file->blocklens and flat_file->indices 
       * (flat_file->count)
       *    blocklens is in bytes
       *    indices is offset from start of the type in bytes
       *    NOTE: start and interaction of type LB ? 
       * */

      dataptr = buf; 
      todo = transfersize; 
      while (todo)
      {
	 for (i=0; todo && i<flat_file->count; ++i)
	 {
	    /* size of cont part in bytes */
	    int itemsize = 
	       min_offset(flat_file->blocklens[i], todo); 

	    /* absolute start of write region in file in bytes */
	    int fileofs = writestart + flat_file->indices[i]; 


	    if (callback->startfragment)
	       callback->startfragment (fileofs, itemsize, data); 

	    status = callback->processdata (dataptr, itemsize, fileofs, data); 

	    if (callback->stopfragment)
	       callback->stopfragment (status, data); 

	    /* advance memory pointer */ 
	    dataptr += itemsize; 
	    todo -= itemsize; 

	    if (!status)
	       break; 
	 }
	 
	 if (!status)
	    break; 
	 
	 writestart += filetype_extent; 
      }
   }
      
   if (callback->stop) 
      callback->stop (status, data); 

   return status; 
}


/****************************************** debug code **********************/
static int dumpfunc (void * membuf, int size, ADIO_Offset fileoffset, void * data)
{
   fprintf (stderr, "typehelper_processtype_debug: mem %p size %u going to file @ %lld\n", 
         membuf, size, fileoffset); 
   return 1; 
}

static void startfunc (void * data)
{
   fprintf (stderr, "start of processing\n"); 
}

static void stopfunc (int status, void * data)
{
   fprintf (stderr, "stop of processing\n"); 
}

static void startfragment (ADIO_Offset ofs, ADIO_Offset size, void * data)
{
   fprintf (stderr, "Start fragment of size %llu at %llu\n",  size, ofs); 
}

static void stopfragment (int status, void * data)
{
   fprintf (stderr, "stop fragment\n"); 
}
void typehelper_processtypes_debug (MPI_Datatype memtype, void * buf, int count, 
      MPI_Datatype filetype, MPI_Datatype etype, int offset,int displacement)
{
   DatatypeHandler handler; 
   handler.processdata = dumpfunc; 
   handler.start = startfunc; 
   handler.stop = stopfunc; 
   handler.startfragment = startfragment; 
   handler.stopfragment = stopfragment; 
   typehelper_processtypes (memtype, buf, count, filetype, etype, 
	 offset, displacement, &handler, 0); 
}
/****************************************************************************/

void typehelper_processoperation (MPI_Datatype memtype, void * buf, int count, 
      ADIO_File fd, MPI_Offset offset, int file_ptr_type, 
      const DatatypeHandler * callback, void * data, int debug)
{
   int memcontig; 
   int filecontig;
   MPI_Count transfersize;
   
   /* performance check */
   MPI_Type_size_x (memtype, &transfersize); 
   if (0 == transfersize * count)
      return; 

   ADIOI_Datatype_iscontig (memtype, &memcontig); 
   ADIOI_Datatype_iscontig (fd->filetype, &filecontig); 

   /* calculate offset, in bytes */
   switch (file_ptr_type)
   {
      case ADIO_EXPLICIT_OFFSET:
	 assert (offset >= 0); 
	 offset *= fd->etype_size; 
	 break;
      case ADIO_INDIVIDUAL:
	 offset = fd->fp_ind; 
	 /* fh->fp_ind is already in bytes */ 
	 break; 
      default:
	 /* shared filepointer is handled in higher level */
	 assert (0 /* UNHANDLED CASE */); 
	 break; 
   };

   if (debug)
      typehelper_processtypes_debug (memtype, buf, count, 
	 fd->filetype, fd->etype, offset, fd->disp); 
     else
      typehelper_processtypes (memtype, buf, count, 
	 fd->filetype, fd->etype, offset, fd->disp,
	 callback, data); 
}
