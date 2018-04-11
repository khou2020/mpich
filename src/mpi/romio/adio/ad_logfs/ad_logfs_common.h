#ifndef AD_LOGFS_COMMON_H
#define AD_LOGFS_COMMON_H

/* Check str for TRUE, true, 1, ... ; Return the bool value of the string
 * contents */
int ad_logfs_checkbool(const char *str);

/* return true if the hint was set; if so, adjust val */
int ad_logfs_hint_bool(MPI_Info info, const char *key, int *val);

int ad_logfs_hint_int(MPI_Info info, const char *key, int *val);

int ad_logfs_hint_str(MPI_Info info, const char *key, char **str);

/* Setting hints */
void ad_logfs_hint_set_bool(MPI_Info info, const char *key, int val);
void ad_logfs_hint_set_int(MPI_Info info, const char *key, int val);
void ad_logfs_hint_set_str(MPI_Info info, const char *key, const char *str);

#endif
