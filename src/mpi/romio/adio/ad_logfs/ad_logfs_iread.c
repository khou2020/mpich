/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_iread.c,v 1.3 2002/10/24 17:01:04 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"

/* ADIOI_LOGFS_IreadContig()
 *
 * Implemented by immediately calling ReadContig()
 */
void ADIOI_LOGFS_IreadContig(ADIO_File fd, void *buf, int count,
                             MPI_Datatype datatype, int file_ptr_type,
                             ADIO_Offset offset, ADIO_Request * request, int
                             *error_code)
{
    int myrank, nprocs, typesize;
    void *handle;

    *error_code = MPI_SUCCESS;

/*    *request = ADIOI_Malloc_request();
    (*request)->optype = ADIOI_WRITE;
    (*request)->fd = fd;
    (*request)->queued = 0;
    (*request)->datatype = datatype;
*/
    MPI_Type_size(datatype, &typesize);
    MPI_Comm_size(fd->comm, &nprocs);
    MPI_Comm_rank(fd->comm, &myrank);
/*    FPRINTF(stdout, "[%d/%d]    calling ADIOI_LOGFS_ReadContig\n",
	    myrank, nprocs);

    len = count * typesize;
    ADIOI_LOGFS_ReadContig(fd, buf, len, MPI_BYTE, file_ptr_type,
			    offset, &status, error_code);

#ifdef HAVE_STATUS_SET_BYTES
    if (*error_code == MPI_SUCCESS) {
	MPI_Get_elements(&status, MPI_BYTE, &len);
	(*request)->nbytes = len;
    }
#endif
    fd->async_count++;*/

    handle = ADIOI_Layer_switch_in(fd);
    fd->fns->ADIOI_xxx_IreadContig(fd, buf, count, datatype, file_ptr_type, offset, request,
                                   error_code);
    ADIOI_Layer_switch_out(fd, handle);
}

void ADIOI_LOGFS_IreadStrided(ADIO_File fd, void *buf, int count,
                              MPI_Datatype datatype, int file_ptr_type,
                              ADIO_Offset offset, ADIO_Request * request, int
                              *error_code)
{
    int myrank, nprocs;
#ifdef HAVE_STATUS_SET_BYTES
    /* int typesize; */
#endif
    void *handle;

/*    *error_code = MPI_SUCCESS;

    *request = ADIOI_Malloc_request();
    (*request)->optype = ADIOI_WRITE;
    (*request)->fd = fd;
    (*request)->queued = 0;
    (*request)->datatype = datatype;
*/
    MPI_Comm_size(fd->comm, &nprocs);
    MPI_Comm_rank(fd->comm, &myrank);
    FPRINTF(stdout, "[%d/%d] ADIOI_LOGFS_IreadStrided called on %s\n",
            myrank, nprocs, fd->filename);
/*
    ADIOI_LOGFS_ReadStrided(fd, buf, count, datatype, file_ptr_type,
			     offset, &status, error_code);

#ifdef HAVE_STATUS_SET_BYTES
    if (*error_code == MPI_SUCCESS) {
	MPI_Type_size(datatype, &typesize);
	(*request)->nbytes = count * typesize;
    }
#endif
    fd->async_count++;*/

    handle = ADIOI_Layer_switch_in(fd);
    fd->fns->ADIOI_xxx_IreadStrided(fd, buf, count, datatype, file_ptr_type,
                                    offset, request, error_code);
    ADIOI_Layer_switch_out(fd, handle);

}
