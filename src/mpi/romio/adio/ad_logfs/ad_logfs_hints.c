/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_logfs.hints.c,v 1.4 2002/10/24 17:01:04 gropp Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_logfs.h"
#include "adioi.h"
#include "layered.h"
#include "logfs.h"

void ADIOI_LOGFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code)
{
    *error_code = MPI_SUCCESS;


    /* In standalone mode we have complete control over hints, so just
     * process the users' info structure and return;
     * However, somehow we still need to call the gen_setinfo
     * since otherwise there are segmentation faults in other parts
     * of the code that depend on certaing things to be set
     * (two-phase for example) */
    if (logfs_standalone(fd)) {

        /* this modifies fd->info */
        ADIOI_GEN_SetInfo(fd, users_info, error_code);

        if (fd->fs_ptr) {
            /* We have a fs_ptr, so update our internal values and
             * update  fd->info (which is return by MPI_File_get_info) */
            logfs_setinfo(fd, users_info);
            return;
        }
        else {
            /* no fs-ptr yet; This must mean that we're in the process
             * of opening a new file in standalone mode;
             * ADIO_SetInfo is called *BEFORE* ADIO_Open is called;
             * AS such, we don't have a fs_ptr structure, and we don't have
             * anywhere to store our filesystem specific hints (except
             * if we modify the union in ADIO_File and pollute it with
             * fs-specific members;
             * Instead we store transfer our hints into fd->info,
             * and call ADIO_SetInfo once more from within ADIO_Open*/
            logfs_transfer_hints(users_info, fd->info);
        }
        *error_code = MPI_SUCCESS;
        return;
    }

    /* special case here:
     * (not applicable for standalone mode)
     *
     *   setinfo is called from within ADIO_Open (which returns the fd struct)
     *   so we cannot call ADIOI_Layer_init until after the call to ADIO_Open
     *   returns.
     *
     *   The only function that is called from within open is the Setinfo
     *   call, so here we need to take into consideration that ADIO_Layer_init
     *   is not yet called; A sign of this is that fs_ptr is 0
     */

    /* if we have a slave, pass the setinfo call, if not use the generic
     * function to update the info argument which will be passed to the slave
     * once it is set */
    if (fd->fs_ptr && ADIOI_Layer_is_slave_set(fd)) {
        void *handle = ADIOI_Layer_switch_in(fd);
        fd->fns->ADIOI_xxx_SetInfo(fd, users_info, error_code);
        ADIOI_Layer_switch_out(fd, handle);
    }
    else {
        /* this basically stores the hint in fd->info; we reuse this hint
         * the moment we can call the slave's open; we call set_info on the slave
         * (which takes a copy of the hint) and free the original hint*/
        ADIOI_GEN_SetInfo(fd, users_info, error_code);
    }
}
