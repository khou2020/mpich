/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_fcntl.c,v 1.8 2004/11/01 21:36:58 robl Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include <assert.h>
#include "logfs.h"
#include "ad_logfs.h"
#include "adioi.h"
#include "adio_extern.h"
#include "layered.h"

void ADIOI_LOGFS_Fcntl(ADIO_File fd, int flag, ADIO_Fcntl_t * fcntl_struct, int *error_code)
{
    static char myname[] = "ADIOI_LOGFS_FCNTL";

    *error_code = MPI_SUCCESS;

    switch (flag) {
    case ADIO_FCNTL_GET_FSIZE:
        /* return filesize from memory */
        fcntl_struct->fsize = logfs_getfsize(fd);
        break;
    case ADIO_FCNTL_SET_DISKSPACE:
        /* change into set_size */
        /* problem here: if size < current size, nothing happens
         * with preallocate (so it is different from set_size)
         *
         * Will be problem in wr-only mode and withy tracking filesize,
         * since getfsize will be illegal in this case */
        if (logfs_getfsize(fd) < fcntl_struct->fsize)
            logfs_resize(fd, fcntl_struct->fsize);
        *error_code = MPI_SUCCESS;
        break;
    case ADIO_FCNTL_SET_ATOMICITY:
        assert(0 /*CANNOT GET HERE */);
        /* the upper level code traps this and disables logfs */
        break;
    default:
        *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                           MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__,
                                           MPI_ERR_ARG, "**flag", "**flag %d", flag);
    }
}
