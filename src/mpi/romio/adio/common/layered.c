#include "layered.h"

int ADIOI_Layer_SetInfo (ADIO_File fd, MPI_Info users_info, 
      int * error_code)
{
   if (!fd->fs_ptr)
   {
      /* this means we are being called from within the ADIO_Open function 
       * and we don't have the slave yet */ 
    
      /* add the keys to the info argument */
      assert (fd->info == MPI_INFO_NULL); 
      MPI_Info_dup (users_info, &fd->info); 

      return 1; 
   }
   return 0; 
}

int ADIOI_Layer_fcntl (ADIO_File fd, int flag, ADIO_Fcntl_t * fcntl_struct,
      int * error_code)
{
   ADIOI_Layer_data * ld;

   if (ADIO_FCNTL_SET_SLAVE != flag)
      return 0; 

   /* fcntl_struct points to the ADIOI_Fns_struct of the slave
    * make a copy of the structure 
    */
  
   /* validate that this fd is ready for layered operations */ 
   ADIOI_Layer_validate (fd); 
   ld = (ADIOI_Layer_data *) fd->fs_ptr; 

   /* check that we didn't have this call already */ 
   // assert (0 == ld->slave_ops); 

   /* the slave is set in ADIO_Layer_init */ 
   assert (ld->slave_ops); 


   *error_code = MPI_SUCCESS; 
   return 1; 
}

void ADIOI_Layer_init (ADIO_File fd, ADIOI_Fns * fns, void * masterdata, 
      int * error_code, int already_open)
{
   void * handle; 
   ADIOI_Layer_data * layerdata = ADIOI_Malloc (sizeof(ADIOI_Layer_data)); 

   layerdata->magic = ROMIO_LAYER_MAGIC; 
   layerdata->master_data = masterdata;
   
   /* save original fns pointer for malloc/free consistency issues */
   layerdata->orig_fns = fd->fns; 

   /* if already_openened, the fd->fns points to the slave ops,
    * if not, fd->fns points to the masters ops */
   layerdata->master_ops = (ADIOI_Fns*) ADIOI_Malloc (sizeof (ADIOI_Fns));
   *layerdata->master_ops = (already_open ? *fns : *fd->fns); 

   layerdata->slave_data = fd->fs_ptr; 
   layerdata->slave_ops = (ADIOI_Fns*) ADIOI_Malloc (sizeof (ADIOI_Fns));
   /* take copy */ 
   /* if already_openened, the slave is already active and fd->fns points to
    * the slave ops */
   *layerdata->slave_ops = (already_open ? *fd->fns : *fns);  

   fd->fs_ptr = layerdata; 
   
   /* --- now the layering is initialized --- */ 

   if (!already_open)
   {
      /* open the slave */ 
      handle = ADIOI_Layer_switch_in (fd); 
      fd->fns->ADIOI_xxx_Open (fd, error_code); 
      ADIOI_Layer_switch_out (fd, handle); 

      if (MPI_SUCCESS == error_code)
      {
         /* inform master that slave is opened and ready */
         fd->fns->ADIOI_xxx_Fcntl (fd, ADIO_FCNTL_SET_SLAVE, 
               0, error_code); 
      }
   }
   else
   {
      fd->fns = layerdata->master_ops; 
   }
}

void * ADIOI_Layer_done (ADIO_File fd)
{
   void * ret ; 
   ADIOI_Layer_validate (fd); 

   /* switchin restores the slave and returns the ADIOI_Layer_data struct */
   //ADIOI_Layer_data * d = ADIOI_Layer_switch_in (fd); 
   ADIOI_Layer_data * d = (ADIOI_Layer_data *) fd->fs_ptr; 
   ret = d->master_data; 

   fd->fns = d->orig_fns; 
   *fd->fns = *d->slave_ops; 

   ADIOI_Free (d->master_ops); 
   ADIOI_Free (d->slave_ops); 
   ADIOI_Free (d); 
   return ret; 
}


int ADIOI_Layer_is_slave_set (ADIO_File fd)
{
   ADIOI_Layer_validate (fd); 
   return (0 != ((ADIOI_Layer_data*) fd->fs_ptr)->slave_ops); 
}

