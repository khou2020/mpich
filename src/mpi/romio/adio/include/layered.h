#ifndef ROMIO_LAYERED_H_INCLUDE
#define ROMIO_LAYERED_H_INCLUDE

#include <assert.h>
#include "adio.h"
#include "adioi.h"

#define ROMIO_LAYER_MAGIC 12396541

/**
 * Support functions for creating layering adio drivers
 *
 * The layered drivers cannot change ADIO_File members 
 * and should always use the layer access functions'
 *   (layer_data, switchin, switchout)
 */

typedef struct
{
   int          magic;        /* error detection */ 
   void *       master_data;  /* data for the outer driver */
   void *       slave_data;   /* original data of the slave driver */
   ADIOI_Fns *  master_ops;   /* operations structure of the master */                   
   ADIOI_Fns *  slave_ops;    /* operations structure of the slave */
   ADIOI_Fns *  orig_fns;     /* original fns pointer */
} ADIOI_Layer_data; 


/*
 * Check the magic to verify this is actually a layered FD
 */
static inline void ADIOI_Layer_validate (ADIO_File fd)
{
   assert (fd->fs_ptr &&
        ((ADIOI_Layer_data*)fd->fs_ptr)->magic == ROMIO_LAYER_MAGIC);  
}

/* return the data pointer of the outer driver
 * Cannot be called when the slave driver is SwitchedIn */
static inline void * ADIOI_Layer_get_data (ADIO_File fd)
{
   ADIOI_Layer_validate (fd); 
   return ((ADIOI_Layer_data*)fd->fs_ptr)->master_data; 
}

static inline void * ADIOI_Layer_set_data (ADIO_File fd, void * data)
{
   ADIOI_Layer_validate (fd); 
   ((ADIOI_Layer_data*)fd->fs_ptr)->master_data = data;  
   return data; 
}

/*
 * Restore the ADIO_File structure so that slave functions can be called;
 * Restores the fs_ptr pointer & the operations pointer
 * Returns a void * pointer that should be passed to SwitchIn to restore the
 * outer(master) driver
 */
static inline void * ADIOI_Layer_switch_in (ADIO_File fd)
{
   ADIOI_Layer_data * d;
   
   ADIOI_Layer_validate (fd); 
   d = (ADIOI_Layer_data*)fd->fs_ptr; 

   fd->fs_ptr = d->slave_data; 
   fd->fns = d->slave_ops; 
   return d; 
}

/*
 * Restore the master driver
 */
static inline void  ADIOI_Layer_switch_out (ADIO_File fd, void * data)
{
   ADIOI_Layer_data * d = (ADIOI_Layer_data*) data;
   

   // d = (ADIOI_Layer_data*)fd->fs_ptr; 

   /* we assume the operations on the slave DO NOT change the address 
    * of the fs_ptr or the fns struct */
   assert (fd->fs_ptr == d->slave_data); 
   assert (fd->fns == d->slave_ops); 

   fd->fs_ptr = d; 
   fd->fns = d->master_ops; 
   ADIOI_Layer_validate (fd); 
}

/*
 * Prepare a valid ADIO_File for layering;
 *
 * There are two modes: already_open = 1 or already_open = 0 
 *
 *  if already_open = 0, it is assumed that the master is in control
 *  en the slave hasn't been active/openened yet.
 *
 *  In this case, the layering is done with the specified driver
 *  as a slave. A copy of the Fns structure passed to the function will 
 *  be used. 
 *  When the function returns, the master driver will be active
 *  Also, the slave will be openened and the
 *  SET_SLAVE fcntl will be called on the master
 *
 * If already_opened is 1, fns is assumed to point to the MASTER
 * and fd is should be an valid filehandle already opened by the slave.
 * In this case the slave open (and SET_SLAVE fcntl) will not be called.
 *
 * In both cases the master is active when the function returns 
 */
void ADIOI_Layer_init (ADIO_File fd, ADIOI_Fns * fns, void * masterdata,
      int * error_code, int already_open); 

/*
 * Remove the layering from the ADIO_File structure
 * When the function returns, the slave driver will be active
 * Returns the void * data of the master driver
 */
void * ADIOI_Layer_done (ADIO_File fd);


/* returns TRUE if the trace layer processed the event 
 * (in this case the calling function should also return),
 * FALSE if the event was not handled and the caller should handle the event
 */
int ADIOI_Layer_fcntl (ADIO_File fd, int flag, ADIO_Fcntl_t * fcntl_struct,
      int * error_code); 

int ADIOI_Layer_SetInfo (ADIO_File fd, MPI_Info users_info, 
      int * error_code); 

/*
 * Return true if the slave is set and the switch functions can be called 
 */
int ADIOI_Layer_is_slave_set (ADIO_File fd); 

#endif
