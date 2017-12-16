#include <malloc.h>
#include "growvector.h"

void growvector_clear (growvector_handle handle)
{
   handle->size = 0; 
}


void growvector_free (growvector_handle * handle)
{
   free ((*handle)->data); 
   free(*handle);
   *handle = 0; 
}

growvector_handle growvector_create (int elesize, int cap)
{
   growvector_handle handle = (growvector_handle)
      malloc (sizeof(struct growvector_instance)); 

   assert (elesize); 

   handle->data =0; 
   handle->size = 0; 
   handle->capacity = 0; 
   handle->elesize = elesize; 

   return handle; 
}

int growvector_reserve (growvector_handle handle, int wanted, int strict)
{
   if (!strict && handle->capacity > wanted)
      return handle->capacity; 

   assert (wanted >= handle->size); 

   handle->data = realloc (handle->data, handle->elesize * wanted); 
   assert (handle->data); 
   handle->capacity = wanted; 

   return handle->capacity; 
}

void growvector_grow (growvector_handle handle)
{
   growvector_reserve (handle, (handle->capacity ? handle->capacity*2
            : (GROWVECTOR_MINSIZE / handle->elesize)), 0); 
   assert (handle->capacity > handle->size); 
}
