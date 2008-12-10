/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_getsh.c,v 1.2 2002/10/24 17:01:04 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"

void ADIOI_LOGFS_Get_shared_fp(ADIO_File fd, int size, ADIO_Offset * shared_fp, int *error_code)
{
    void *handle;

    *error_code = MPI_SUCCESS;

    /* not supported */
    assert(0);

    /* do we need to log set shared/ get shared?
     *   probably not */

    /* will this work correctly when seeking past the end of the file?
     * if not, we need to make setsize calls before doing this */

    handle = ADIOI_Layer_switch_in(fd);
    ADIO_Get_shared_fp(fd, size, shared_fp, error_code);
    ADIOI_Layer_switch_out(fd, handle);
}
