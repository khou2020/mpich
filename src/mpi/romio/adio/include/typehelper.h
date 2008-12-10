#ifndef ROMIO_TYPEHELPER_H
#define ROMIO_TYPEHELPER_H


#include "adio.h"

/*
 * Helper functions for dealing with datatypes 
 *
 *
 */

/* all can be 0 except processdata */
typedef struct
{
   /* start processing */
   void (*start) (void * data);

   /* start of continuous region in the second datatype 
    * (will usually be the filetype) */
   void (*startfragment) (ADIO_Offset fileoffset, ADIO_Offset fragmentsize,
         void * data); 
   
   /* return zero to stop processing */ 
   int (*processdata) (void * membuf, int size, ADIO_Offset fileoffset, void *
         data); 
  
   /* end of contiguous region in second filetype
    * status = 0 if processing was cancelled */ 
   void (*stopfragment) (int status, void * data); 

   /* enf of processing for these types
    * status = 0 if processing was cancelled */
   void (*stop) (int status, void * data); 
} DatatypeHandler; 

/* call callback to process the data stream; Displacement is in BYTES in the
 * file (not etype extents) */
int typehelper_processtypes (MPI_Datatype memtype, void  * buf, int count, 
      MPI_Datatype filetype, MPI_Datatype etype, int offset, 
      int displacement, const DatatypeHandler * callback, void * data); 

/* debug helper: just dump parts to the screen */
void typehelper_processtypes_debug (MPI_Datatype memtype, void * buf, 
      int count, MPI_Datatype filetype,MPI_Datatype etype, int offset, 
      int displacement);

/* calculate file offset, and call processtypes  */ 
void typehelper_processoperation (MPI_Datatype memtype, void * buf, int count, 
      ADIO_File fd, MPI_Offset offset, int file_ptr_type, 
      const DatatypeHandler * callback, void * data, int debug); 

void typehelper_processtypes_debug (MPI_Datatype memtype,
	void * buf, int count, MPI_Datatype filetype,
	MPI_Datatype etype, int offset,int displacement);
/* stream the datatype contents (meant for memory datatypes)
 * In the callback functions, ofs will refer to the linear offset within the
 * datatype at which this fragment starts (so ofs runs from 0 .. 
 * (MPI_Type_size - 1)*/
int typehelper_decodememtype (const void * buf, int count, MPI_Datatype memtype,
      const DatatypeHandler * callback, void * data); 


/* 
 * Calculate the first and last bytes in the file affected by
 * a write with these parameters
 */
void typehelper_calcrange (MPI_Datatype etype, MPI_Datatype ftype,
      ADIO_Offset disp, ADIO_Offset ofs, int writesize, ADIO_Offset * start, 
      ADIO_Offset * stop); 

/* 
 * Iterate over continuous regions in the file access pattern 
 * offset given in etypes
 */
void typehelper_calcaccess (MPI_Datatype etype, 
      MPI_Datatype ftype, ADIO_Offset disp, ADIO_Offset offset, 
      int writesize, const DatatypeHandler * cb, void * userdata); 

#endif
