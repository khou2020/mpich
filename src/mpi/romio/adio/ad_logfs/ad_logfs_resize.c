/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_resize.c,v 1.2 2002/10/24 17:01:05 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"
#include "logfs.h"

void ADIOI_LOGFS_Resize(ADIO_File fd, ADIO_Offset size, int *error_code)
{
    int myrank, nprocs;

    /* resize always works */
    *error_code = MPI_SUCCESS;

    MPI_Comm_size(fd->comm, &nprocs);
    MPI_Comm_rank(fd->comm, &myrank);
    FPRINTF(stdout, "[%d/%d] ADIOI_LOGFS_Resize called on %s\n", myrank, nprocs, fd->filename);

    logfs_resize(fd, size);
}
