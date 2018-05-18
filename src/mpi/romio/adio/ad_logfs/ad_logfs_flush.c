/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_flush.c,v 1.2 2002/10/24 17:01:04 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"
#include "logfs.h"

void ADIOI_LOGFS_Flush(ADIO_File fd, int *error_code)
{
    void *handle;

    *error_code = MPI_SUCCESS;

    /* When reading is possible, forced to do log replay here to
     * adhere to MPI file consistency rules
     *
     * If not, just force everything to the logfile
     */
    /* flush */
    logfs_flush(fd);

    if (!logfs_standalone(fd)) {
        /* also flush the real file */
        handle = ADIOI_Layer_switch_in(fd);
        fd->fns->ADIOI_xxx_Flush(fd, error_code);
        ADIOI_Layer_switch_out(fd, handle);
    }
}