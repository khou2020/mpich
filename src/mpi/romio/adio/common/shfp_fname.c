/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"

static unsigned int xorshift_rand(void)
{
    /* time returns long; keep the lower and most significant 32 bits */
    unsigned int val = time(NULL) & 0xffffffff;

    /* Marsaglia's xorshift random number generator */
    val ^= val << 13;
    val ^= val >> 17;
    val ^= val << 5;

    return val;
}

/*@ MPL_create_pathname - Generate a random pathname

Input Parameters:
+   dirname - String containing the path of the parent dir (current dir if NULL)
+   prefix - String containing the prefix of the generated name
-   is_dir - Boolean to tell if the path should be treated as a directory

Output Parameters:
.   dest_filename - String to copy the generated path name

    Notes:
    dest_filename should point to a preallocated buffer of PATH_MAX size.

  Module:
  Utility
  @*/
void MPL_create_pathname(char *dest_filename, const char *dirname,
                         const char *prefix, const int is_dir)
{
    /* Generate a random number which doesn't interfere with user application */
    const unsigned int random = xorshift_rand();
    const unsigned int pid = (unsigned int) getpid();

    if (dirname) {
        MPL_snprintf(dest_filename, PATH_MAX, "%s/%s.%u.%u%c", dirname, prefix,
                     random, pid, is_dir ? '/' : '\0');
    } else {
        MPL_snprintf(dest_filename, PATH_MAX, "%s.%u.%u%c", prefix,
                     random, pid, is_dir ? '/' : '\0');
    }
}

/*@ MPL_strnapp - Append to a string with a maximum length

Input Parameters:
+   instr - String to copy
-   maxlen - Maximum total length of 'outstr'

Output Parameters:
.   outstr - String to copy into

    Notes:
    This routine is similar to 'strncat' except that the 'maxlen' argument
    is the maximum total length of 'outstr', rather than the maximum
    number of characters to move from 'instr'.  Thus, this routine is
    easier to use when the declared size of 'instr' is known.

  Module:
  Utility
  @*/
int MPL_strnapp(char *dest, const char *src, size_t n)
{
    char *mpl_restrict d_ptr = dest;
    const char *mpl_restrict s_ptr = src;
    register int i;

    /* Get to the end of dest */
    i = (int) n;
    while (i-- > 0 && *d_ptr)
        d_ptr++;
    if (i <= 0)
        return 1;

    /* Append.  d_ptr points at first null and i is remaining space. */
    while (*s_ptr && i-- > 0) {
        *d_ptr++ = *s_ptr++;
    }

    /* We allow i >= (not just >) here because the first while decrements
     * i by one more than there are characters, leaving room for the null */
    if (i >= 0) {
        *d_ptr = 0;
        return 0;
    } else {
        /* Force the null at the end */
        *--d_ptr = 0;

        /* We may want to force an error message here, at least in the
         * debugging version */
        return 1;
    }
}

/* The following function selects the name of the file to be used to
   store the shared file pointer. The shared-file-pointer file is a
   hidden file in the same directory as the real file being accessed.
   If the real file is /tmp/thakur/testfile, the shared-file-pointer
   file will be /tmp/thakur/.testfile.shfp.yyy.xxxx, where yyy
   is rank 0's process id and xxxx is a random number. If the
   underlying file system supports shared file pointers
   (PVFS does not, for example), the file name is always
   constructed. This file is created only if the shared
   file pointer functions are used and is deleted when the real
   file is closed. */

void ADIOI_Shfp_fname(ADIO_File fd, int rank, int *error_code)
{
    int len;
    char *slash, *ptr, tmp[PATH_MAX];

    fd->shared_fp_fname = (char *) ADIOI_Malloc(PATH_MAX);

    if (!rank) {
        MPL_create_pathname(tmp, NULL, ".shfp", 0);

        if (ADIOI_Strncpy(fd->shared_fp_fname, fd->filename, PATH_MAX)) {
            *error_code = ADIOI_Err_create_code("ADIOI_Shfp_fname", fd->filename, ENAMETOOLONG);
            return;
        }
#ifdef ROMIO_NTFS
        slash = strrchr(fd->filename, '\\');
#else
        slash = strrchr(fd->filename, '/');
#endif
        if (!slash) {
            if (ADIOI_Strncpy(fd->shared_fp_fname, ".", 2)) {
                *error_code = ADIOI_Err_create_code("ADIOI_Shfp_fname", fd->filename, ENAMETOOLONG);
                return;
            }
            if (ADIOI_Strncpy(fd->shared_fp_fname + 1, fd->filename, PATH_MAX - 1)) {
                *error_code = ADIOI_Err_create_code("ADIOI_Shfp_fname", fd->filename, ENAMETOOLONG);
                return;
            }
        } else {
            ptr = slash;
#ifdef ROMIO_NTFS
            slash = strrchr(fd->shared_fp_fname, '\\');
#else
            slash = strrchr(fd->shared_fp_fname, '/');
#endif
            if (ADIOI_Strncpy(slash + 1, ".", 2)) {
                *error_code = ADIOI_Err_create_code("ADIOI_Shfp_fname", fd->filename, ENAMETOOLONG);
                return;
            }
            /* ok to cast: file names bounded by PATH_MAX and NAME_MAX */
            len = (int) (PATH_MAX - (slash + 2 - fd->shared_fp_fname));
            if (ADIOI_Strncpy(slash + 2, ptr + 1, len)) {
                *error_code = ADIOI_Err_create_code("ADIOI_Shfp_fname", ptr + 1, ENAMETOOLONG);
                return;
            }
        }

        /* MPL_strnapp will return non-zero if truncated.  That's ok */
        MPL_strnapp(fd->shared_fp_fname, tmp, PATH_MAX);

        len = (int) strlen(fd->shared_fp_fname);
        MPI_Bcast(&len, 1, MPI_INT, 0, fd->comm);
        MPI_Bcast(fd->shared_fp_fname, len + 1, MPI_CHAR, 0, fd->comm);
    } else {
        MPI_Bcast(&len, 1, MPI_INT, 0, fd->comm);
        MPI_Bcast(fd->shared_fp_fname, len + 1, MPI_CHAR, 0, fd->comm);
    }
}
