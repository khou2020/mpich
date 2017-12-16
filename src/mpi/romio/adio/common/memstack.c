#include <assert.h>
#include <malloc.h>
#include "memstack.h"

struct  memstack_memblock
{
   int                         size; /* nb of elements in block */ 
   int                         used; /* nb of used elements inblock */
   struct memstack_memblock *  prev; 
   struct memstack_memblock *  next; 
   char *                     data; 
};

typedef struct memstack_memblock memstack_memblock; 


struct memstack_instance
{
   int size;     /* nb of elements in stack */
   int elesize;  /* bytes / element */
   int spare;    /* spare capacity */

   memstack_memblock * mem;  /* points to current head block */

}; 

typedef struct memstack_instance memstack_instance; 


memstack_handle memstack_create (int elesize)
{
   memstack_handle ret = malloc (sizeof(memstack_instance)); 
   assert (ret); 

   ret->size = 0; 
   ret->elesize = elesize; 
   ret->mem = 0; 
   ret->spare = 0; 
   return ret; 
}

int memstack_getsize (memstack_consthandle handle)
{
   return handle->size; 
}

static void memstack_addblock (memstack_handle handle)
{
   int blocksize = (MEMSTACK_BLOCKSIZE * 1024) / (handle->elesize); 
   assert (blocksize); 

   char * newmem = malloc (blocksize*handle->elesize); 
   memstack_memblock * newnode = malloc (sizeof(memstack_memblock)); 
   
   newnode->data = newmem; 
   newnode->size = blocksize; 
   newnode->prev = 0; 
   newnode->next = 0; 
   newnode->used = 0; 

   handle->spare += blocksize; 

   if (!handle->mem)
   {
      handle->mem = newnode; 
      return;
   }

   /* add block after current  head */
   newnode->prev = handle->mem; 
   newnode->next = handle->mem->next; 
   handle->mem->next = newnode; 
   if (newnode->next)
      newnode->next->prev = newnode; 
}

void memstack_reducemem (memstack_handle handle)
{
   memstack_memblock * cur = handle->mem; 
   

   /* since mem always pointss to the current head, only need to look forward
    * */
   while (cur)
   {
      memstack_memblock * tmp; 

      /* see if we can free remove the block; if not, try the next one */
      if (cur->used)
      {
         cur = cur->next;
         continue; 
      }

      /* we can free the current block */
      tmp = cur; 
      cur = cur->next; 

      free (tmp->data); 
      if (tmp->prev)
         tmp->prev->next = tmp->next;
      if (tmp->next)
         tmp->next->prev = tmp->prev; 
      
      handle->spare -= tmp->size; 
      free (tmp); 
   }
   
   /* only way the head gets freed if is the total size is 0 */
   if (!handle->size)
      handle->mem = 0; 
}

/* erase all elements, keeping memory blocks (to reduce memory, 
 * call memstack_reducemem) */
void memstack_clear (memstack_handle handle)
{
   memstack_memblock * cur = handle->mem; 

   if (!handle->size)
      return; 

   /* find most left (prev) block, set the root pointer to it
    * and clear count */
   assert (cur); 

   while (cur && cur->prev)
      cur=cur->prev; 

   handle->mem = cur; 
   handle->spare += handle->size; 
   handle->size = 0; 
}

void memstack_free (memstack_handle * handle)
{
   memstack_clear (*handle); 
   memstack_reducemem (*handle); 

   assert (0==(*handle)->size); 
   assert (0==(*handle)->spare); 

   free ((*handle)); 
   *handle = 0; 
}

char * memstack_push (memstack_handle handle)
{
   char * ret; 

   if (!handle->mem || (handle->mem->used == handle->mem->size))
   {
      assert (0==handle->spare); 
      memstack_addblock (handle); 
      assert (handle->spare == handle->mem->size); 
      assert (0==handle->mem->used); 
   }
      
   assert (handle->mem); 
   assert (handle->spare); 

   ret = handle->mem->data + (handle->elesize * handle->mem->used);

   ++handle->mem->used;
   ++handle->size; 
   --handle->spare; 

   return ret; 
}

char * memstack_pop (memstack_handle handle)
{
   char * ret = 0; 
   assert (handle->size); 
   if (!handle->size)
      return ret; 

   assert (handle->mem); 
   assert (handle->mem->used); 

   /* first decrease page usage counter and use as index in page */
   --handle->mem->used; 
   ret = handle->mem->data + (handle->mem->used * handle->elesize);
   --handle->size; 
   ++handle->spare; 

   if (!handle->mem->used && handle->mem->prev)
      handle->mem = handle->mem->prev; 

   return ret; 
}
