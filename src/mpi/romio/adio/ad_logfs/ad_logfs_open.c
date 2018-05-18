/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_testfs_open.c,v 1.2 2002/10/24 17:01:04 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include <assert.h>
#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"
#include "logfs.h"

/* Open is only called when we are in complete control
 * (e.g. not through set_view ("logfs") */

void ADIOI_LOGFS_Open(ADIO_File fd, int *error_code)
{
    int ret;
    *error_code = MPI_SUCCESS;

    ret = logfs_activate (fd, fd->info);
    if (ret == MPI_SUCCESS) {
	/* need to set view so that an entry is made in the logfile describing
	 * our default view*/
	logfs_set_view(fd, 0, MPI_BYTE, MPI_BYTE);
    } else {
	*error_code = ADIOI_Err_create_code("ADIOI_LOGFS_Open",
		fd->filename, ret);
    }
}