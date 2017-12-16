#ifndef LOGFS_GROWVECTOR_H
#define LOGFS_GROWVECTOR_H

#include <assert.h>
#include <string.h>

/* always allocate at least 128 bytes */
#define GROWVECTOR_MINSIZE 128

struct growvector_instance; 

typedef struct growvector_instance * growvector_handle; 
typedef const struct growvector_instance * growvector_consthandle; 

growvector_handle growvector_create (int elesize, int cap);
void growvector_free (growvector_handle * handle); 

void growvector_clear (growvector_handle handle); 

/* make sure there is room for at least elesize elements */
/* returns capacity 
 * If reduce is true, reductions will be allowed*/
int growvector_reserve (growvector_handle handle, int elesize, int reduce); 

/* make more room in vector */
void growvector_grow (growvector_handle handle); 

/* return pointer to element */
static inline void * growvector_get (growvector_handle handle, int ele); 
static inline void * growvector_pushback_checked (growvector_handle handle, void *
      ele); 

static inline int growvector_elesize (growvector_consthandle handle); 

#ifndef NDEBUG
#define growvector_pushback(a,b,c) \
   growvector_pushback_checked ((a), (b)); assert (c==growvector_elesize((a))); 
#else
#define growvector_pushback(a,b,c) \ \
   growvector_pushback_checked ((a), (b)); 
#endif

static inline int growvector_size (growvector_consthandle handle); 
static inline int growvector_capacity (growvector_consthandle handle); 
/* -- implementation issues -- */

struct growvector_instance 
{
   char * data; 
   int capacity; 
   int size; 
   int elesize; 
}; 

static inline int growvector_elesize (growvector_consthandle handle)
{
   return handle->elesize; 
}

/* Return pointer to start of memory, 0 if vector is empty */ 
static inline void * growvector_get_null (growvector_handle handle)
{
   return (handle->size ? handle->data : 0); 
}

static inline void * growvector_get (growvector_handle handle, int ele)
{
   assert (ele < handle->size); 
   return handle->data + (handle->elesize * ele); 
}

static inline int growvector_size (growvector_consthandle handle)
{
   return handle->size; 
}

static inline int growvector_capacity (growvector_consthandle handle)
{
   return handle->capacity; 
}

static inline void * growvector_pushback_checked (growvector_handle handle, void *
      data)
{
   void * mem; 
   if (handle->capacity == handle->size)
      growvector_grow (handle); 
   assert (handle->size < handle->capacity); 

   /* hope GCC/compiler is smart here if elesize <= 32 ... */
   ++handle->size; 
   mem = growvector_get(handle, handle->size-1);
   memcpy (mem, data, handle->elesize); 
   return mem;
}
#endif
