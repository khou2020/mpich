/*
 * Public Header file supporting user (i.e. not inside MPI/ROMIO) replay of
 * logfs files
 */

#ifndef LOGFS_USER_H
#define LOGFS_USER_H

typedef struct {
    int (*init) ();
    int (*done) ();
} logfs_user_replay_cb;

#endif
