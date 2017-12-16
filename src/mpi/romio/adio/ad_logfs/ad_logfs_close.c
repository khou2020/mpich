/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_close.c,v 1.2 2002/10/24 17:01:03 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"
#include "logfs.h"



void ADIOI_LOGFS_Close(ADIO_File fd, int *error_code)
{
    int standalone;

    standalone = logfs_standalone(fd);

    /* deactivate logfs ;
     * If replay_on_close is set, there will first be a replay */
    logfs_deactivate(fd);

    *error_code = MPI_SUCCESS;

    /* layering (if any) is disabled in logfs_deactivate
     * so we just need to call the close function of the slave */
    if (!standalone)
        fd->fns->ADIOI_xxx_Close(fd, error_code);
}
