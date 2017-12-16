/*
 *  Auto-growing unordered temp storage structure 
 */
#ifndef MEMSTACK_H
#define MEMSTACK_H


/* Allocation granularity of memstack, in kb */
#define MEMSTACK_BLOCKSIZE 1

struct memstack_instance; 

typedef struct memstack_instance * memstack_handle; 
typedef const struct memstack_instance * memstack_consthandle; 

memstack_handle memstack_create (int elesize);

void memstack_free (memstack_handle * handle); 

char * memstack_push (memstack_handle handle);
char * memstack_pop (memstack_handle handle); 

int memstack_getsize (memstack_consthandle handle); 

/* remove all elements */
void memstack_clear (memstack_handle handle); 

/* reduce mem if possible */
void memstack_reducemem (memstack_handle handle); 

#endif
