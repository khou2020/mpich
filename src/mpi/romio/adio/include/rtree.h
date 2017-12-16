#ifndef ROMIO_RTREE_H
#define ROMIO_RTREE_H

#include "rtree_config.h"

#ifndef RTREE_RANGE_TYPE
#define No RTREE_RANGE_TYPE set!
#endif

#ifndef RTREE_DATA_TYPE
#error No RTREE_DATA_TYPE set!
#endif

/* 
 * options
 *    RTREE_SORT_NODES
 *    RTREE_SORT_ENTRIES
 */
/* #define RTREE_INIT_MEM */ 


#define RTREE_CLEAR_MEM


/* needs operator < for sorting */
#define RTREE_SORT_NODES
#define RTREE_SORT_ENTRIES

struct rtree_node; 

struct rtree; 

typedef struct 
{
   RTREE_RANGE_TYPE start; 
   RTREE_RANGE_TYPE stop; 
} rtree_range; 

typedef struct rtree * rtree_handle; 
typedef const struct rtree * rtree_const_handle; 

typedef int (*rtree_callback) (const rtree_range * range, RTREE_DATA_TYPE * data, 
      void * extra); 

typedef void (*rtree_callback_split) (const rtree_range ** sources, 
      int * mapping, int count, void * extra); 

typedef void (*rtree_callback_copy) (const rtree_range * range, 
      RTREE_DATA_TYPE * data, RTREE_DATA_TYPE * newdata);

typedef struct
{
   const rtree_range * range; 
   RTREE_DATA_TYPE *   data; 
   int                 depth; 
   int                 treedepth;
   void *              nodeid; 
   void *              parentid; 
   void *              extra; 
   rtree_const_handle  tree; 
} rtree_callback_all_info; 

typedef int (*rtree_callback_all) (const rtree_callback_all_info * info); 

/* manipulation */
rtree_handle rtree_create (); 

void rtree_free (rtree_handle * rtree); 

void rtree_add (rtree_handle tree, const rtree_range * range, 
      RTREE_DATA_TYPE data); 

/* 
 * Call function for every rectangle overlapping  range 
 * Continue walking the tree as long as the callback return true
 *  Return false if the callback returned false, else return true
 */ 
int rtree_overlap (rtree_const_handle, const rtree_range * range, 
                rtree_callback callback, void * extra); 

/* return the span range of the tree */ 
void rtree_get_range (rtree_const_handle tree, rtree_range  * range); 

/* return the depth of the tree */ 
int rtree_get_depth (rtree_const_handle tree); 

/* Return maximum number of children in a node */ 
int rtree_get_child_max (rtree_const_handle tree); 

/* return the minimum number of children in a node */
int rtree_get_child_min (rtree_const_handle tree); 

/* return the number of elements in the tree */
int rtree_get_count (rtree_const_handle tree); 

/*
 * Call the callback for every node in the tree.
 * Stop when the callback returns false.
 * Returns true if all nodes were visited, 
 * false if stopped early because the callback returned false
 * Passes extra to the callback 
 */
int rtree_walk (rtree_const_handle tree, rtree_callback callback, void * extra); 


int rtree_walk_all (rtree_const_handle tree, rtree_callback_all callback, void *
      data); 

/* for debugging: write internal tree structure to the screen */ 
void rtree_dump (rtree_const_handle tree);

/* for debugging; validate consistenty of node extents */ 
int rtree_check (rtree_const_handle tree); 

/* search for specific range; returns pointer to data if found, 0 otherwise */
RTREE_DATA_TYPE * rtree_find (rtree_const_handle tree, 
      const rtree_range * range); 


/* set callback to be called on entry removal */
void rtree_set_freefunc (rtree_handle tree, rtree_callback func, void *
      extra); 

/* set callback to decide how nodes are split */
void rtree_set_splitfunc (rtree_handle tree, rtree_callback_split func, void *
      data); 

/* try to remove the specified range from the tree; 
 * The exact range must be present; If found, remove and
 * return TRUE; In addition, if data is not 0, data will be
 * set to the data value of the range. Returns FALSE
 * if the range cannot be found
 */
int rtree_remove (rtree_handle tree, const rtree_range * range, 
      RTREE_DATA_TYPE * data); 

/**** TODO
 
  -> range delete
  -> range contain : alle ranges volledig in opgegeven range

  ***/


/*
 * Copy the whole tree; Optionally calling rtree_callback_copy 
 * to copy the object data; If copy is NULL, data values will
 * be copied to the new tree
 */
rtree_handle rtree_copy (rtree_const_handle tree, rtree_callback_copy copy,
      void * extra); 


void rtree_clear (rtree_handle tree); 

/* return true if the tree is empty */ 
int rtree_empty (rtree_const_handle tree); 

/******************** iterator functions **************************/
/* TODO make inline */


struct rtree_iterator;

typedef struct rtree_iterator *             rtree_iterator_handle; 
typedef const struct rtree_iterator *       rtree_const_iterator_handle; 

const RTREE_DATA_TYPE * rtree_iterator_get_item_data (rtree_const_iterator_handle iter);

const rtree_range * rtree_iterator_get_item_range (rtree_const_iterator_handle iter);

void rtree_iterator_forward (rtree_iterator_handle); 

void rtree_iterator_backward (rtree_iterator_handle); 

rtree_iterator_handle rtree_iterator_create (rtree_handle tree); 

#endif
